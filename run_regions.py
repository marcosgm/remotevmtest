"""Run ingestion or paired notebooks over SSH, preserving results outside Azure."""

import argparse
import base64
import concurrent.futures
import gzip
import hashlib
import json
import shlex
import subprocess

from provision import ACCOUNT, CONFIG, GROUP, KEY, LOCAL_REGION, REMOTE_REGION, ROOT, STATE, az

KNOWN_HOSTS = str(KEY.parent / "benchmark_known_hosts")
SSH_OPTIONS = [
    "-i", str(KEY), "-o", "StrictHostKeyChecking=yes",
    "-o", "UserKnownHostsFile=" + KNOWN_HOSTS,
    "-o", "ConnectTimeout=20", "-o", "ServerAliveInterval=30",
]


def host(region):
    record = json.loads((STATE / f"{region}-provision.json").read_text())
    return "bench@" + record["publicIpAddress"]


def remote(region, command, logfile, timeout):
    print(f"{region}: {command}", flush=True)
    with (STATE / logfile).open("w") as log:
        result = subprocess.run(
            ["ssh", *SSH_OPTIONS, host(region), command],
            stdout=log, stderr=subprocess.STDOUT, text=True, timeout=timeout,
        )
    print((STATE / logfile).read_text(), flush=True)
    return result.returncode


def retrieve(region, remote_path, local_path):
    subprocess.run(
        ["scp", *SSH_OPTIONS, host(region) + ":" + remote_path, str(local_path)],
        check=True,
    )


def retrieve_run_command(region, remote_path, local_path):
    """Export a completed artifact through the VM agent without opening inbound ports."""
    record = json.loads((STATE / f"{region}-provision.json").read_text())
    chunks = []
    count = 1
    expected_hash = None
    while len(chunks) < count:
        index = len(chunks)
        code = (
            "import base64,gzip,hashlib,json,pathlib;"
            f"raw=pathlib.Path({remote_path!r}).read_bytes();"
            "encoded=base64.b64encode(gzip.compress(raw,mtime=0)).decode();"
            f"print(json.dumps(dict(chunk=encoded[{index * 2800}:{(index + 1) * 2800}],"
            "count=(len(encoded)+2799)//2800,sha256=hashlib.sha256(raw).hexdigest())))"
        )
        response = az(
            "vm", "run-command", "invoke", "-g", GROUP, "-n", record["name"],
            "--command-id", "RunShellScript", "--scripts", "python3 -c " + shlex.quote(code),
        )
        (STATE / f"{region}-{local_path.name}-export-{index}.json").write_text(json.dumps(response, indent=2))
        packets = [
            json.loads(line)
            for value in response["value"] for line in value["message"].splitlines()
            if line.startswith('{"chunk":')
        ]
        if len(packets) != 1:
            raise RuntimeError(f"VM-agent artifact export returned no unique packet: {response}")
        packet = packets[0]
        if expected_hash is not None and packet["sha256"] != expected_hash:
            raise RuntimeError("Remote artifact changed during export")
        count, expected_hash = packet["count"], packet["sha256"]
        chunks.append(packet["chunk"])
        print(f"{region}: exported {local_path.name} chunk {index + 1}/{count}", flush=True)
    payload = gzip.decompress(base64.b64decode("".join(chunks)))
    if hashlib.sha256(payload).hexdigest() != expected_hash:
        raise RuntimeError("VM-agent artifact export hash mismatch")
    local_path.write_bytes(payload)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["ingest", "measure", "collect", "recover"])
    args = parser.parse_args()
    if args.phase == "ingest":
        status = remote(
            LOCAL_REGION,
            "cd /home/bench && /home/bench/venv/bin/python ingest.py "
            "--auth managed_identity --dataset-dir /mnt/locality/source",
            "ingest-output.txt", 2400,
        )
        retrieve(LOCAL_REGION, f"/home/bench/evidence/{CONFIG['run']}/ingest.json", STATE / "ingest.json")
        if status:
            raise RuntimeError("Dataset ingestion failed; inspect ingest-output.txt")
        retrieve(LOCAL_REGION, "/home/bench/manifest.json", "manifest.json")
    elif args.phase == "measure":
        def execute(region):
            status = remote(
                region,
                f"cd /home/bench && BENCH_REGION={shlex.quote(region)} BENCH_ACCOUNT={shlex.quote(ACCOUNT)} "
                f"BENCH_VM_SIZE={shlex.quote(CONFIG['vm_size'])} "
                f"BENCH_TRIALS={CONFIG['trials']} BENCH_EPOCHS={CONFIG['epochs']} "
                "OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 "
                "/home/bench/venv/bin/python execute_notebook.py",
                f"{region}-notebook-output.txt", 7200,
            )
            retrieve(region, "/mnt/locality/metrics.json", STATE / f"{region}-metrics.json")
            retrieve(
                region, f"/home/bench/blob-locality-{region}-executed.ipynb",
                f"blob-locality-{region}-executed.ipynb",
            )
            if status:
                raise RuntimeError(f"Notebook failed in {region}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(execute, [LOCAL_REGION, REMOTE_REGION]))
    elif args.phase == "recover":
        retrieve_run_command(
            REMOTE_REGION, "/mnt/locality/metrics.json", STATE / f"{REMOTE_REGION}-metrics.json",
        )
        retrieve_run_command(
            REMOTE_REGION, f"/home/bench/blob-locality-{REMOTE_REGION}-executed.ipynb",
            ROOT / f"blob-locality-{REMOTE_REGION}-executed.ipynb",
        )
    else:
        for region in [LOCAL_REGION, REMOTE_REGION]:
            retrieve(region, "/mnt/locality/metrics.json", STATE / f"{region}-metrics.json")


if __name__ == "__main__":
    main()
