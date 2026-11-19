"""Instrumented stage-once/local-reuse benchmark, usable from Jupyter or the CLI."""

import collections
import concurrent.futures
import csv
import datetime as dt
import hashlib
import io
import json
import os
import pathlib
import platform
import subprocess
import tarfile
import threading
import time
import urllib.parse
import uuid

import numpy as np
import requests

API_VERSION = "2023-11-03"
VERSIONS = ["6.7", "6.8", "6.9", "6.10", "6.12"]
NON_LETTERS = bytes(x for x in range(256) if not (65 <= x <= 90 or 97 <= x <= 122))


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


class BlobClient:
    """One explicit HTTP attempt per logged operation; failures abort the job."""

    def __init__(self, account, auth="managed_identity"):
        self.endpoint = f"https://{account}.blob.core.windows.net"
        self.auth = auth
        self.operations = []
        self.lock = threading.Lock()
        self.token = None
        self.expires = 0

    def authenticate(self):
        if self.token and time.time() + 120 < self.expires:
            return
        if self.auth == "managed_identity":
            session = requests.Session()
            session.trust_env = False
            response = session.get(
                "http://169.254.169.254/metadata/identity/oauth2/token",
                params={"api-version": "2018-02-01", "resource": "https://storage.azure.com/"},
                headers={"Metadata": "true"}, timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            self.token, self.expires = data["access_token"], int(data["expires_on"])
        elif self.auth == "azure_cli":
            data = json.loads(subprocess.check_output([
                "az", "account", "get-access-token", "--resource",
                "https://storage.azure.com/", "-o", "json",
            ], text=True))
            self.token = data["accessToken"]
            self.expires = int(data["expires_on"])
        else:
            raise ValueError(f"Unknown authentication mode {self.auth}")

    def request(self, method, container, blob="", params=None, data=None, headers=None, destination=None):
        self.authenticate()
        url = f"{self.endpoint}/{container}"
        if blob:
            url += "/" + urllib.parse.quote(blob, safe="/")
        request_id = str(uuid.uuid4())
        h = {
            "Authorization": f"Bearer {self.token}",
            "x-ms-version": API_VERSION,
            "x-ms-client-request-id": request_id,
            **(headers or {}),
        }
        operation = (
            "ListBlobs" if params and params.get("comp") == "list"
            else "PutBlockList" if params and params.get("comp") == "blocklist"
            else "PutBlock" if params and params.get("comp") == "block"
            else "GetBlob" if method == "GET"
            else "PutBlob" if method == "PUT"
            else method
        )
        event = {
            "started": utc(), "operation": operation, "method": method,
            "container": container, "blob": blob, "client_request_id": request_id,
            "request_body_bytes": len(data) if isinstance(data, bytes) else 0,
            "response_body_bytes": 0,
        }
        started = time.perf_counter()
        try:
            # A fresh session makes each Blob TTFB include its own DNS/TCP/TLS setup.
            with requests.Session() as session:
                with session.request(
                    method, url, params=params, data=data, headers=h,
                    stream=True, timeout=(30, 180), allow_redirects=False,
                ) as response:
                    event["status"] = response.status_code
                    event["server_request_id"] = response.headers.get("x-ms-request-id")
                    event["headers_seconds"] = time.perf_counter() - started
                    if response.status_code >= 400:
                        event["azure_error"] = response.content.decode("utf-8", errors="replace")[:2000]
                    response.raise_for_status()
                    if not 200 <= response.status_code < 300:
                        raise RuntimeError(f"Unexpected Blob HTTP status {response.status_code}")
                    event["tier"] = response.headers.get("x-ms-access-tier")
                    first = response.raw.read(1)
                    event["ttfb_body_seconds"] = time.perf_counter() - started if first else None
                    output = destination.open("wb") if destination else io.BytesIO()
                    try:
                        output.write(first)
                        event["response_body_bytes"] += len(first)
                        while chunk := response.raw.read(1024 * 1024):
                            output.write(chunk)
                            event["response_body_bytes"] += len(chunk)
                        if destination:
                            output.flush()
                            os.fsync(output.fileno())
                        result = b"" if destination else output.getvalue()
                    finally:
                        output.close()
                    length = response.headers.get("Content-Length")
                    if length is not None and event["response_body_bytes"] != int(length):
                        raise RuntimeError(f"Truncated response for {blob}")
                    event["seconds"] = time.perf_counter() - started
                    event["payload_MiB_s"] = event["response_body_bytes"] / 2**20 / event["seconds"]
                    return result, event
        except (requests.RequestException, OSError, RuntimeError) as error:
            event["error"] = str(error)
            raise
        finally:
            event.setdefault("seconds", time.perf_counter() - started)
            with self.lock:
                self.operations.append(event)

    def put(self, container, name, payload, tier="Cold"):
        return self.request(
            "PUT", container, name, data=payload,
            headers={"x-ms-blob-type": "BlockBlob", "x-ms-access-tier": tier},
        )


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def stage(client, manifest, directory, workers=4):
    directory.mkdir(parents=True, exist_ok=True)
    client.authenticate()
    started = time.perf_counter()

    def download(item):
        path = directory / item["name"]
        _, event = client.request("GET", "dataset", item["name"], destination=path)
        if path.stat().st_size != item["bytes"]:
            raise RuntimeError(f"Length mismatch for {item['name']}")
        if sha256_file(path) != item["sha256"]:
            raise RuntimeError(f"SHA256 mismatch for {item['name']}")
        if event["tier"] != "Cold":
            raise RuntimeError(f"{item['name']} is not Cold tier: {event['tier']}")
        return event

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        events = list(pool.map(download, manifest))
    seconds = time.perf_counter() - started
    return {
        "seconds": seconds, "bytes": sum(x["bytes"] for x in manifest),
        "MiB_s_including_hash_validation": sum(x["bytes"] for x in manifest) / 2**20 / seconds,
        "workers": workers, "blob_events": events,
    }


def filename_features(name):
    lower = name.lower()
    base = lower.rsplit("/", 1)[-1]
    return [
        1, len(base), len(lower), lower.count("/"), base.count("."),
        sum(c.isdigit() for c in base),
        *[base.count(chr(97 + i)) for i in range(26)],
    ]


def preprocess(manifest, directory, output):
    started = time.perf_counter()
    files = total_bytes = letters = lines = 0
    with output.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["path", "bytes", "letters", "lines"])
        for item in manifest:
            # Streaming tar members avoids unsafe filesystem extraction and still
            # opens every logical file from the locally staged compressed archive.
            with tarfile.open(directory / item["name"], "r|gz") as archive:
                for member in archive:
                    if not member.isfile():
                        continue
                    size = file_letters = file_lines = 0
                    with archive.extractfile(member) as content:
                        while chunk := content.read(1024 * 1024):
                            size += len(chunk)
                            file_letters += len(chunk.translate(None, NON_LETTERS))
                            file_lines += chunk.count(b"\n")
                    if size != member.size:
                        raise RuntimeError(f"Member size mismatch: {member.name}")
                    relative = member.name.split("/", 1)[1]
                    writer.writerow([relative, size, file_letters, file_lines])
                    files += 1
                    total_bytes += size
                    letters += file_letters
                    lines += file_lines
            print(f"Processed {item['name']}: {files:,} files", flush=True)
        stream.flush()
        os.fsync(stream.fileno())
    return {
        "seconds": time.perf_counter() - started, "files": files,
        "uncompressed_bytes": total_bytes, "letters": letters, "lines": lines,
        "features_bytes": output.stat().st_size,
    }


def train_epoch(features_path, output_path):
    """Fit ridge regression on log(size); group by filename to avoid version leakage."""
    started = time.perf_counter()
    train_x, train_y, test_x, test_y = [], [], [], []
    with features_path.open(newline="") as stream:
        for row in csv.DictReader(stream):
            name, size = row["path"], int(row["bytes"])
            test = hashlib.sha256(name.encode()).digest()[0] < 51
            (test_x if test else train_x).append(filename_features(name))
            (test_y if test else train_y).append(np.log1p(size))
    x, y = np.asarray(train_x, dtype=float), np.asarray(train_y)
    tx, ty = np.asarray(test_x, dtype=float), np.asarray(test_y)
    scale = np.maximum(x.std(axis=0), 1)
    x /= scale
    tx /= scale
    weights = np.linalg.solve(x.T @ x + np.eye(x.shape[1]), x.T @ y)
    predicted_log = tx @ weights
    rmse_log = float(np.sqrt(np.mean((predicted_log - ty) ** 2)))
    mae_bytes = float(np.mean(np.abs(np.expm1(predicted_log) - np.expm1(ty))))
    model = {
        "weights": weights.tolist(), "scale": scale.tolist(),
        "rmse_log1p_bytes": rmse_log, "mae_bytes": mae_bytes,
        "train_rows": len(y), "test_rows": len(ty),
        "sample_filename": "drivers/gpu/drm/drm_file.c",
        "sample_predicted_bytes": float(np.expm1(
            np.asarray(filename_features("drivers/gpu/drm/drm_file.c")) / scale @ weights
        )),
    }
    with output_path.open("w") as stream:
        json.dump(model, stream)
        stream.flush()
        os.fsync(stream.fileno())
    return {"seconds": time.perf_counter() - started, **model}


def net_bytes():
    values = {}
    for path in pathlib.Path("/sys/class/net").iterdir():
        if path.name != "lo":
            values[path.name] = {
                name: int((path / "statistics" / name).read_text())
                for name in ["rx_bytes", "tx_bytes"]
            }
    return values


def delta_net(before, after):
    return {
        interface: {key: counts[key] - before[interface][key] for key in counts}
        for interface, counts in after.items() if interface in before
    }


def environment(workdir, label, expected_vm_size=None):
    if label != "local-controller" and not expected_vm_size:
        raise ValueError("An expected VM SKU is required for regional measurements")
    data = {
        "label": label, "time": utc(), "platform": platform.platform(),
        "python": platform.python_version(), "numpy": np.__version__,
        "cpu_count": os.cpu_count(),
        "cpu_model": next(
            (line.split(":", 1)[1].strip() for line in pathlib.Path("/proc/cpuinfo").read_text().splitlines()
             if line.startswith("model name")), "unknown",
        ),
        "workdir": str(workdir),
        "mount": subprocess.check_output(["findmnt", "-T", str(workdir), "-J"], text=True),
        "lsblk": subprocess.check_output(["lsblk", "-J", "-o", "NAME,SIZE,TYPE,MOUNTPOINTS,MODEL"], text=True),
    }
    if label != "local-controller":
        session = requests.Session()
        session.trust_env = False
        r = session.get(
            "http://169.254.169.254/metadata/instance",
            params={"api-version": "2021-02-01"}, headers={"Metadata": "true"}, timeout=10,
        )
        r.raise_for_status()
        compute = r.json()["compute"]
        data["azure_vm"] = {k: compute.get(k) for k in [
            "location", "vmSize", "name", "offer", "sku", "version", "osType",
        ]}
        if compute["location"].lower() != label or compute["vmSize"] != expected_vm_size:
            raise RuntimeError(f"Unexpected VM placement: {data['azure_vm']}")
        target = json.loads(data["mount"])["filesystems"][0]["target"]
        if target != "/mnt":
            raise RuntimeError(f"Workdir is not on the temporary SSD mounted at /mnt: {target}")
    return data


def run_experiment(account, manifest_path, workdir, label, auth="managed_identity", trials=3, epochs=3,
                   expected_vm_size=None):
    workdir = pathlib.Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(pathlib.Path(manifest_path).read_text())
    client = BlobClient(account, auth)
    result = {
        "schema_version": 1, "started": utc(),
        "measurement_type": "regional_vm",
        "environment": environment(workdir, label, expected_vm_size), "trials": [],
        "notes": [
            "Application HTTP payload bytes exclude TLS/TCP/IP headers and retransmissions.",
            "TTFB is time to first response-body byte, including connection setup, not storage server latency.",
            "Each trial downloads every blob afresh; each local epoch reuses its local feature CSV.",
            "No Blob retries or redirects are hidden. Authentication and initial configuration are outside timed job.",
            "Local repetitions may use the OS page cache; no global cache dropping on shared hosts.",
        ],
    }
    client.authenticate()
    try:
        for trial in range(1, trials + 1):
            directory = workdir / f"trial-{trial}"
            directory.mkdir(exist_ok=True)
            before = net_bytes()
            started = time.perf_counter()
            record = {"trial": trial, "started": utc()}
            record["stage"] = stage(client, manifest, directory)
            local_before = net_bytes()
            operations_before = len(client.operations)
            record["preprocess"] = preprocess(manifest, directory, directory / "features.csv")
            record["epochs"] = [
                train_epoch(directory / "features.csv", directory / f"model-{i}.json")
                for i in range(1, epochs + 1)
            ]
            record["local_blob_calls"] = len(client.operations) - operations_before
            record["local_interface_delta"] = delta_net(local_before, net_bytes())
            payload = json.dumps({
                "region": label, "trial": trial, "preprocess": record["preprocess"],
                "model": record["epochs"][-1],
            }, sort_keys=True).encode()
            upload_started = time.perf_counter()
            client.put("results", f"{label}/trial-{trial}.json", payload)
            record["upload"] = {
                "seconds": time.perf_counter() - upload_started, "bytes": len(payload),
                "calls": 1, "tier": "Cold",
            }
            record["total_seconds"] = time.perf_counter() - started
            record["interface_delta"] = delta_net(before, net_bytes())
            result["trials"].append(record)
            print(
                f"{label} trial {trial}: stage={record['stage']['seconds']:.3f}s "
                f"local={record['preprocess']['seconds'] + sum(x['seconds'] for x in record['epochs']):.3f}s "
                f"upload={record['upload']['seconds']:.3f}s total={record['total_seconds']:.3f}s",
                flush=True,
            )
    finally:
        result["finished"] = utc()
        result["operations"] = client.operations
        result["api_call_counts"] = dict(collections.Counter(x["operation"] for x in client.operations))
        (workdir / "metrics.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def run_local_validation(manifest_path, input_directory, workdir, epochs=2):
    """Validate the ML pipeline only; never describe this output as an Azure benchmark."""
    manifest = json.loads(pathlib.Path(manifest_path).read_text())
    directory = pathlib.Path(input_directory)
    workdir = pathlib.Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    for item in manifest:
        path = directory / item["name"]
        if path.stat().st_size != item["bytes"] or sha256_file(path) != item["sha256"]:
            raise RuntimeError(f"Local input integrity failure: {item['name']}")
    result = {
        "measurement_type": "offline_pipeline_validation_only",
        "started": utc(), "environment": environment(workdir, "local-controller"),
        "regional_comparison": False, "blob_http_calls": 0,
    }
    result["preprocess"] = preprocess(manifest, directory, workdir / "features.csv")
    result["epochs"] = [
        train_epoch(workdir / "features.csv", workdir / f"model-{i}.json")
        for i in range(1, epochs + 1)
    ]
    result["finished"] = utc()
    (workdir / "offline-metrics.json").write_text(json.dumps(result, indent=2) + "\n")
    return result
