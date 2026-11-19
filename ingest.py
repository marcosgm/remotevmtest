"""Download public Linux releases and upload them directly into Cold Blob tier."""

import concurrent.futures
import argparse
import hashlib
import json
import pathlib
import subprocess
import time
import uuid

import requests

from benchmark import BlobClient, VERSIONS, sha256_file, utc
from provision import ACCOUNT, GROUP, STATE, SUBSCRIPTION, az

ROOT = pathlib.Path(__file__).resolve().parent
DATA = ROOT / ".dataset"
BLOCK = 16 * 1024 * 1024


def grant_controller():
    token = json.loads(subprocess.check_output(
        ["az", "account", "get-access-token", "-o", "json"], text=True,
    ))["accessToken"]
    import base64
    claims = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=="))
    scope = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{GROUP}/providers/Microsoft.Storage/storageAccounts/{ACCOUNT}"
    az("role", "assignment", "create", "--assignee-object-id", claims["oid"],
       "--assignee-principal-type", "User", "--role", "Storage Blob Data Contributor",
       "--scope", scope)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--download-only", action="store_true",
                        help="Prepare public inputs locally without attempting Azure access.")
    parser.add_argument("--auth", choices=["azure_cli", "managed_identity"], default="azure_cli")
    parser.add_argument("--dataset-dir", type=pathlib.Path, default=DATA)
    args = parser.parse_args()
    args.dataset_dir.mkdir(parents=True, exist_ok=True)
    client = None
    if not args.download_only:
        if args.auth == "azure_cli":
            grant_controller()
        client = BlobClient(ACCOUNT, args.auth)
        client.authenticate()
    manifest = []
    checksum_response = requests.get(
        "https://cdn.kernel.org/pub/linux/kernel/v6.x/sha256sums.asc", timeout=60,
    )
    checksum_response.raise_for_status()
    checksums = {}
    for line in checksum_response.text.splitlines():
        parts = line.split()
        if len(parts) == 2 and len(parts[0]) == 64:
            checksums[parts[1].lstrip("*")] = parts[0]
    try:
        for version in VERSIONS:
            name = f"linux-{version}.tar.gz"
            source = f"https://cdn.kernel.org/pub/linux/kernel/v6.x/{name}"
            path = args.dataset_dir / name
            started = time.perf_counter()
            expected_hash = checksums[name]
            if not path.exists() or sha256_file(path) != expected_hash:
                temporary = path.with_suffix(path.suffix + ".part")
                subprocess.run(
                    ["curl", "--http1.1", "--fail", "--location", "--silent", "--show-error",
                     "--retry", "3", "--retry-all-errors", "--max-time", "600",
                     "--output", str(temporary), source], check=True,
                )
                if sha256_file(temporary) != expected_hash:
                    raise RuntimeError(f"Upstream SHA-256 mismatch: {name}")
                temporary.replace(path)
            item = {
                "name": name, "source": source, "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
                "checksum_source": "https://cdn.kernel.org/pub/linux/kernel/v6.x/sha256sums.asc",
                "source_download_seconds": time.perf_counter() - started,
                "target_tier": "Cold", "uploaded": False,
            }
            if args.download_only:
                manifest.append(item)
                print(f"Prepared local {name}: {item['bytes']:,} bytes", flush=True)
                continue
            block_ids = []
            with path.open("rb") as stream:
                index = 0
                while chunk := stream.read(BLOCK):
                    import base64
                    block_id = base64.b64encode(f"{index:08d}".encode()).decode()
                    client.request("PUT", "dataset", name,
                                   params={"comp": "block", "blockid": block_id}, data=chunk)
                    block_ids.append(block_id)
                    index += 1
            xml = (
                '<?xml version="1.0" encoding="utf-8"?><BlockList>'
                + "".join(f"<Latest>{block_id}</Latest>" for block_id in block_ids)
                + "</BlockList>"
            ).encode()
            client.request(
                "PUT", "dataset", name, params={"comp": "blocklist"}, data=xml,
                headers={"x-ms-access-tier": "Cold", "Content-Type": "application/xml"},
            )
            item["uploaded"] = True
            manifest.append(item)
            print(f"Uploaded Cold {name}: {item['bytes']:,} bytes", flush=True)
        (ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        total = sum(x["bytes"] for x in manifest)
        if not 1e9 <= total <= 2e9:
            raise RuntimeError(f"Dataset outside requested 1-2 GB: {total}")
        print(f"Dataset complete: {total:,} bytes, {total / 2**30:.4f} GiB", flush=True)
    finally:
        old = STATE / "ingest.json"
        if old.exists():
            old.rename(STATE / f"ingest-attempt-{uuid.uuid4().hex[:8]}.json")
        (STATE / "ingest.json").write_text(json.dumps({
            "finished": utc(), "mode": "download_only" if args.download_only else "azure_ingest",
            "manifest": manifest, "operations": client.operations if client else [],
        }, indent=2) + "\n")


if __name__ == "__main__":
    main()
