import hashlib
import io
import json
import pathlib
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import requests
import nbformat

import discover_regions
import make_notebook
from benchmark import BlobClient, environment, stage


class InstrumentationTests(unittest.TestCase):
    def response(self, payload=b"abc", status=200):
        response = MagicMock()
        response.__enter__.return_value = response
        response.status_code = status
        response.headers = {"Content-Length": str(len(payload)), "x-ms-access-tier": "Cold"}
        response.raw = io.BytesIO(payload)
        response.content = payload
        if status >= 400:
            response.raise_for_status.side_effect = requests.HTTPError(str(status))
        return response

    def client(self):
        client = BlobClient("example")
        client.token, client.expires = "test-only", 9e12
        return client

    @patch("benchmark.requests.Session")
    def test_download_counts_payload_and_records_ttfb(self, session):
        session.return_value.__enter__.return_value.request.return_value = self.response()
        client = self.client()
        with tempfile.TemporaryDirectory() as temp:
            output = pathlib.Path(temp) / "blob"
            client.request("GET", "dataset", "test.tar.gz", destination=output)
            self.assertEqual(output.read_bytes(), b"abc")
        event = client.operations[0]
        self.assertEqual(event["operation"], "GetBlob")
        self.assertEqual(event["response_body_bytes"], 3)
        self.assertGreaterEqual(event["ttfb_body_seconds"], event["headers_seconds"])
        self.assertNotIn("Authorization", str(event))

    @patch("benchmark.requests.Session")
    def test_failed_attempt_is_logged_and_not_retried(self, session):
        call = session.return_value.__enter__.return_value.request
        call.return_value = self.response(b"denied", 403)
        client = self.client()
        with self.assertRaises(requests.HTTPError):
            client.request("PUT", "dataset", "test", params={"comp": "block"}, data=b"abcd")
        self.assertEqual(call.call_count, 1)
        self.assertEqual(len(client.operations), 1)
        self.assertEqual(client.operations[0]["operation"], "PutBlock")
        self.assertEqual(client.operations[0]["status"], 403)
        self.assertEqual(client.operations[0]["azure_error"], "denied")

    def test_staging_rejects_hash_mismatch(self):
        class FakeClient:
            def authenticate(self):
                pass

            def request(self, *args, destination, **kwargs):
                destination.write_bytes(b"bad")
                return b"", {"tier": "Cold"}

        manifest = [{"name": "a", "bytes": 3, "sha256": hashlib.sha256(b"abc").hexdigest()}]
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError, "SHA256 mismatch"):
                stage(FakeClient(), manifest, pathlib.Path(temp), workers=1)

    def test_staging_rejects_non_cold_inputs(self):
        class FakeClient:
            def authenticate(self):
                pass

            def request(self, *args, destination, **kwargs):
                destination.write_bytes(b"abc")
                return b"", {"tier": "Hot"}

        manifest = [{"name": "a", "bytes": 3, "sha256": hashlib.sha256(b"abc").hexdigest()}]
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError, "is not Cold"):
                stage(FakeClient(), manifest, pathlib.Path(temp), workers=1)

    @patch("benchmark.subprocess.check_output")
    @patch("benchmark.requests.Session")
    def test_selected_region_and_sku_are_verified_against_instance_metadata(self, session, command):
        response = session.return_value.get.return_value
        response.json.return_value = {"compute": {
            "location": "eastus2", "vmSize": "Standard_D4ds_v5",
        }}
        command.side_effect = lambda args, **kwargs: (
            json.dumps({"filesystems": [{"target": "/mnt"}]}) if args[0] == "findmnt" else "{}"
        )
        self.assertEqual(
            environment(pathlib.Path("/mnt/locality"), "eastus2", "Standard_D4ds_v5")["azure_vm"]["location"],
            "eastus2",
        )
        with self.assertRaisesRegex(RuntimeError, "Unexpected VM placement"):
            environment(pathlib.Path("/mnt/locality"), "centralus", "Standard_D4ds_v5")
        with self.assertRaisesRegex(RuntimeError, "Unexpected VM placement"):
            environment(pathlib.Path("/mnt/locality"), "eastus2", "Standard_D2ds_v5")
        command.side_effect = lambda args, **kwargs: (
            json.dumps({"filesystems": [{"target": "/"}]}) if args[0] == "findmnt" else "{}"
        )
        with self.assertRaisesRegex(RuntimeError, "not on the temporary SSD"):
            environment(pathlib.Path("/mnt/locality"), "eastus2", "Standard_D4ds_v5")

    def test_regional_measurement_requires_expected_sku(self):
        with self.assertRaisesRegex(ValueError, "expected VM SKU is required"):
            environment(pathlib.Path("/mnt/locality"), "eastus2")


class SelectionTests(unittest.TestCase):
    @patch("discover_regions.subprocess.check_output", return_value='{"accessToken":"test-only"}')
    @patch("discover_regions.requests.get")
    def test_discovery_uses_selected_regions_and_skus(self, get, token):
        get.return_value.json.return_value = {"value": [
            {"name": name, "resourceType": "virtualMachines", "restrictions": []}
            for name in ["Standard_D4ds_v5", "Standard_D2ds_v5"]
        ]}
        for args in [[], ["--regions", "eastus2", "centralus", "--vm-sizes", "Standard_D4ds_v5"]]:
            with self.subTest(args=args), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp)
                with patch.multiple(discover_regions, ROOT=root, SUBSCRIPTION="test-subscription",
                                    REGIONS=["eastus2", "centralus"], CANDIDATES=["Standard_D4ds_v5"]):
                    discover_regions.main(args)
                results = json.loads((root / "evidence/region-discovery/summary.json").read_text())
                self.assertEqual([x["region"] for x in results], ["eastus2", "centralus"])
                self.assertTrue(all(
                    [item["name"] for item in x["candidates"]] == ["Standard_D4ds_v5"]
                    for x in results
                ))
        self.assertTrue(all(
            "--subscription" in call.args[0] and "test-subscription" in call.args[0]
            for call in token.call_args_list
        ))

    def test_notebook_carries_selected_sku_without_display_name_configuration(self):
        config = {
            "run": "test", "storage_region": "eastus2", "remote_region": "centralus",
            "vm_size": "Standard_D4ds_v5",
        }
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            (root / "manifest.json").write_text("[]")
            (root / "benchmark.py").write_text("")
            with patch.multiple(make_notebook, ROOT=root, CONFIG=config):
                make_notebook.main()
            notebook = nbformat.read(root / "blob-locality.ipynb", as_version=4)
        variables = {}
        with patch.dict("os.environ", {}, clear=True):
            exec(notebook.cells[1].source, variables)
        self.assertEqual(variables["VM_SIZE"], "Standard_D4ds_v5")
        self.assertEqual(variables["REGION"], "eastus2")
        self.assertIn("expected_vm_size=VM_SIZE", notebook.cells[4].source)
        self.assertIn("centralus", notebook.cells[0].source)


if __name__ == "__main__":
    unittest.main()
