import copy
import json
import math
import re
import subprocess
import unittest
from unittest.mock import patch

from build_report import CONFIG, REGIONS, ROOT, STATE, build_page, load, report_data, verify
from fetch_prices import vm_hourly_price, vm_query


class RegionalEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = load(ROOT / "manifest.json")
        cls.metrics = load(STATE / f"{REGIONS[1]}-metrics.json")

    def test_completed_replacement_region_evidence_passes(self):
        verify(self.metrics, self.manifest, REGIONS[1])

    def test_wrong_placement_or_incomplete_corpus_is_rejected(self):
        metrics = copy.deepcopy(self.metrics)
        metrics["environment"]["azure_vm"]["location"] = REGIONS[0]
        with self.assertRaisesRegex(RuntimeError, "Wrong region"):
            verify(metrics, self.manifest, REGIONS[1])
        metrics = copy.deepcopy(self.metrics)
        metrics["trials"][0]["stage"]["blob_events"][0]["response_body_bytes"] -= 1
        with self.assertRaisesRegex(RuntimeError, "Byte count mismatch"):
            verify(metrics, self.manifest, REGIONS[1])

    def test_failed_or_extra_blob_attempts_are_rejected(self):
        metrics = copy.deepcopy(self.metrics)
        metrics["operations"][0]["status"] = 403
        with self.assertRaisesRegex(RuntimeError, "Failed Blob operation"):
            verify(metrics, self.manifest, REGIONS[1])
        metrics = copy.deepcopy(self.metrics)
        metrics["operations"].append(copy.deepcopy(metrics["operations"][0]))
        with self.assertRaisesRegex(RuntimeError, "Unexpected Blob operation counts"):
            verify(metrics, self.manifest, REGIONS[1])


class PublicReportTests(unittest.TestCase):
    def test_generic_report_does_not_embed_deployment_identifiers(self):
        data = report_data()
        page = build_page(data).casefold()
        identifiers = [
            *REGIONS, CONFIG.get("storage_region_name", REGIONS[0]),
            CONFIG.get("remote_region_name", REGIONS[1]),
            CONFIG["subscription"], CONFIG["run"],
        ]
        for identifier in identifiers:
            with self.subTest(identifier=identifier):
                self.assertFalse(identifier.casefold() in page, f"Deployment identifier leaked: {identifier}")


class SelectedVMPriceTests(unittest.TestCase):
    def setUp(self):
        self.sku = "Standard_NC4as_T4_v3"
        self.region = "eastus2"
        self.meter = {
            "armSkuName": self.sku, "armRegionName": self.region, "skuName": self.sku,
            "productName": "Virtual Machines NCasT4_v3 Series", "meterName": "NC4as T4 v3",
            "type": "Consumption", "unitOfMeasure": "1 Hour",
            "isPrimaryMeterRegion": True, "retailPrice": 0.5,
        }

    def test_selected_sku_linux_payg_price_excludes_other_offers(self):
        query = vm_query(self.region, self.sku)
        self.assertIn(f"armSkuName eq '{self.sku}'", query)
        self.assertIn(f"armRegionName eq '{self.region}'", query)
        variants = [
            {"productName": "Virtual Machines NCasT4_v3 Series Windows"},
            {"productName": "Virtual Machines NCasT4_v3 Series Red Hat Enterprise Linux"},
            {"skuName": self.sku + " Spot"},
            {"meterName": "NC4as T4 v3 Low Priority"},
            {"armSkuName": "Standard_D2ds_v5"},
            {"armRegionName": "centralus"},
            {"type": "Reservation"},
            {"type": "DevTestConsumption"},
            {"unitOfMeasure": "1/Month"},
        ]
        items = [self.meter, *[{**self.meter, **variant, "retailPrice": 9} for variant in variants]]
        self.assertEqual(vm_hourly_price(items, self.sku, self.region), 0.5)

    def test_missing_or_ambiguous_selected_sku_price_is_an_error(self):
        for items in [[], [{**self.meter, "armSkuName": "Standard_D2ds_v5"}],
                      [self.meter, {**self.meter, "retailPrice": 0.6}]]:
            with self.subTest(items=items), self.assertRaisesRegex(RuntimeError, "Missing/ambiguous"):
                vm_hourly_price(items, self.sku, self.region)

    def test_report_uses_selected_sku_instead_of_fixed_meter_name(self):
        def selected_load(path):
            data = load(path)
            if path.name.endswith("-metrics.json"):
                data["environment"]["azure_vm"]["vmSize"] = self.sku
            elif path.name == "pricing.json":
                for region in REGIONS:
                    data["queries"][f"vm_{region}"]["items"] = [
                        {**self.meter, "armRegionName": region, "retailPrice": 1.25}
                    ]
            return data

        with patch("build_report.CONFIG", {**CONFIG, "vm_size": self.sku}), \
                patch("build_report.load", side_effect=selected_load):
            data = report_data()
        self.assertEqual(data["defaults"]["localVM"], 1.25)
        self.assertEqual(data["defaults"]["remoteVM"], 1.125)


class WorkloadProjectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = report_data()
        script = re.search(r"<script>(.*?)</script>", build_page(cls.data), re.S).group(1)
        cls.model = script.split("function calculate()", 1)[0]

    def calculate(self, **overrides):
        values = {
            "bytes": self.data["size"], "files": self.data["inputBlobs"], "extra": 0,
            "prices": [0.113, 0.1017], "egress": 0.02,
            "jobs": 30, "days": 90, "endpoint": "ephemeral", **overrides,
        }
        result = subprocess.run(
            ["node", "-e",
             "const fs=require('node:fs'),vm=require('node:vm');"
             "const input=JSON.parse(fs.readFileSync(0,'utf8'));"
             "process.stdout.write(JSON.stringify(vm.runInNewContext("
             "input.code+'\\nscenarioCosts('+JSON.stringify(input.values)+')')));"],
            input=json.dumps({"code": self.model, "values": values}),
            text=True, capture_output=True, check=True,
        )
        return json.loads(result.stdout)

    def test_original_workload_preserves_measured_stage_and_total(self):
        for projected, measured in zip(self.calculate(), self.data["profiles"]):
            self.assertAlmostEqual(projected["stage"], measured["stage"])
            self.assertAlmostEqual(projected["seconds"], measured["total"])
            self.assertAlmostEqual(projected["localSeconds"], measured["local"])

    def test_more_files_add_request_wave_and_read_cost_without_more_payload(self):
        baseline = self.calculate()
        increased = self.calculate(files=self.data["inputBlobs"] + self.data["workers"])
        for before, after, profile in zip(baseline, increased, self.data["profiles"]):
            self.assertAlmostEqual(after["stage"] - before["stage"], profile["ttfbMs"] / 1000)
            self.assertAlmostEqual(
                after["requests"] - before["requests"],
                self.data["workers"] * self.data["rates"]["reads10k"] / 10000,
            )
            self.assertEqual(after["egress"], before["egress"])
            self.assertEqual(after["retrieval"], before["retrieval"])

    def test_larger_payload_scales_bytes_without_double_counting_ttfb_or_extra_training(self):
        baseline = self.calculate(extra=1.5)
        doubled = self.calculate(bytes=self.data["size"] * 2, extra=1.5)
        waves = math.ceil(self.data["inputBlobs"] / self.data["workers"])
        for before, after, profile in zip(baseline, doubled, self.data["profiles"]):
            self.assertAlmostEqual(after["retrieval"], before["retrieval"] * 2)
            self.assertEqual(after["requests"], before["requests"])
            self.assertAlmostEqual(after["stage"], before["stage"] * 2 - waves * profile["ttfbMs"] / 1000)
            self.assertAlmostEqual(after["localSeconds"] - 1.5 * 3600, (before["localSeconds"] - 1.5 * 3600) * 2)
        self.assertAlmostEqual(
            doubled[1]["egress"] - baseline[1]["egress"], self.data["size"] / 1e9 * 0.02,
        )


if __name__ == "__main__":
    unittest.main()
