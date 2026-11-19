"""Provision only isolated, short-lived resources for the locality experiment."""

import concurrent.futures
import datetime as dt
import json
import os
import pathlib
import subprocess

import requests

ROOT = pathlib.Path(__file__).resolve().parent
CONFIG = json.loads((ROOT / "experiment.json").read_text())
SUBSCRIPTION = CONFIG["subscription"]
RUN = CONFIG["run"]
STATE = ROOT / "evidence" / RUN
STATE.mkdir(parents=True, exist_ok=True)
GROUP = f"rg-blob-locality-{RUN}"
ACCOUNT = f"stlocality{RUN}"
LOCAL_REGION = CONFIG["storage_region"]
REMOTE_REGION = CONFIG["remote_region"]
KEY = pathlib.Path(os.environ.get("BENCH_SSH_KEY", str(ROOT / ".benchmark-ssh" / "id_ed25519"))).expanduser().resolve()


def az(*args):
    p = subprocess.run(
        ["az", *args, "--subscription", SUBSCRIPTION, "-o", "json",
         "--only-show-errors"],
        text=True, capture_output=True,
    )
    if p.returncode:
        raise RuntimeError(f"az {' '.join(args[:5])}: {p.stderr}")
    return json.loads(p.stdout) if p.stdout.strip() else None


def save(name, value):
    (STATE / name).write_text(json.dumps(value, indent=2) + "\n")


def create_vm(region, short, image, source_ip):
    name = f"vm-{short}"
    started = dt.datetime.now(dt.timezone.utc).isoformat()
    print(f"{region}: creating {name}", flush=True)
    vm = az(
        "vm", "create", "-g", GROUP, "-n", name, "-l", region,
        "--size", CONFIG["vm_size"], "--image", image,
        "--admin-username", "bench", "--ssh-key-values", str(KEY) + ".pub",
        "--storage-sku", "Standard_LRS", "--os-disk-size-gb", "32",
        "--public-ip-sku", "Standard", "--nsg-rule", "NONE",
        "--assign-identity", "--security-type", "TrustedLaunch",
        "--accelerated-networking", "true", "--custom-data", str(ROOT / "cloud-init.yaml"),
        "--tags", "purpose=blob-locality-benchmark", f"run={RUN}",
    )
    detail = az("vm", "show", "-g", GROUP, "-n", name)
    az(
        "network", "nsg", "rule", "create", "-g", GROUP,
        "--nsg-name", f"{name}NSG", "-n", "OnlyBenchmarkController",
        "--priority", "100", "--source-address-prefixes", source_ip + "/32",
        "--destination-port-ranges", "22", "--access", "Allow", "--protocol", "Tcp",
    )
    scope = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{GROUP}/providers/Microsoft.Storage/storageAccounts/{ACCOUNT}"
    az(
        "role", "assignment", "create", "--assignee-object-id",
        detail["identity"]["principalId"], "--assignee-principal-type",
        "ServicePrincipal", "--role", "Storage Blob Data Contributor",
        "--scope", scope,
    )
    record = {
        "region": region, "name": name, "publicIpAddress": vm["publicIpAddress"],
        "size": detail["hardwareProfile"]["vmSize"], "started": started,
        "provisioned": dt.datetime.now(dt.timezone.utc).isoformat(),
        "image": detail["storageProfile"]["imageReference"],
        "principalId": detail["identity"]["principalId"],
        "vnet": f"{name}VNET", "subnet": f"{name}Subnet",
    }
    save(f"{region}-provision.json", record)
    print(f"{region}: ready {vm['publicIpAddress']}", flush=True)
    return record


def main():
    if not all(isinstance(CONFIG[key], str) and CONFIG[key].strip()
               for key in ["storage_region", "remote_region", "vm_size"]):
        raise ValueError("storage_region, remote_region and vm_size must be nonempty strings")
    if LOCAL_REGION.lower() == REMOTE_REGION.lower():
        raise ValueError("Select two different storage_region and remote_region values")
    if not KEY.exists():
        KEY.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(KEY)],
            check=True,
        )
    source_ip = requests.get("https://api.ipify.org", timeout=30).text.strip()
    az("group", "create", "-n", GROUP, "-l", LOCAL_REGION,
       "--tags", "purpose=blob-locality-benchmark", f"run={RUN}")
    save("resources.json", {
        "subscription": SUBSCRIPTION, "group": GROUP, "account": ACCOUNT,
        "run": RUN, "created": dt.datetime.now(dt.timezone.utc).isoformat(),
        "configuration": CONFIG,
    })
    image = az("vm", "image", "show", "-l", LOCAL_REGION,
               "--urn", "Canonical:ubuntu-24_04-lts:server:latest")
    urn = f"Canonical:ubuntu-24_04-lts:server:{image['name']}"
    save("image.json", {"urn": urn})
    print("Pinned image:", urn, flush=True)
    storage = az(
        "storage", "account", "create", "-g", GROUP, "-n", ACCOUNT,
        "-l", LOCAL_REGION, "--sku", "Standard_LRS", "--kind", "StorageV2",
        "--allow-blob-public-access", "false", "--allow-shared-key-access", "false",
        "--min-tls-version", "TLS1_2", "--https-only", "true",
        "--public-network-access", "Disabled",
        "--tags", "purpose=blob-locality-benchmark", f"run={RUN}",
    )
    save("storage.json", {
        "name": storage["name"], "location": storage["location"],
        "sku": storage["sku"], "kind": storage["kind"],
        "endpoint": storage["primaryEndpoints"]["blob"],
        "publicNetworkAccess": storage["publicNetworkAccess"],
    })
    for container in ["dataset", "results"]:
        az("storage", "container-rm", "create", "-g", GROUP,
           "--storage-account", ACCOUNT, "-n", container, "--public-access", "off")
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = [
            pool.submit(create_vm, r, s, urn, source_ip)
            for r, s in [(LOCAL_REGION, "local"), (REMOTE_REGION, "remote")]
        ]
        errors = []
        for region, future in zip([LOCAL_REGION, REMOTE_REGION], futures):
            try:
                future.result()
            except RuntimeError as error:
                save(f"{region}-provision-error.json", {"error": str(error)})
                print(str(error), flush=True)
                errors.append(region)
        if errors:
            raise RuntimeError(f"VM provisioning failed in {errors}; see evidence.")
    scope = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{GROUP}/providers/Microsoft.Storage/storageAccounts/{ACCOUNT}"
    for region in [LOCAL_REGION, REMOTE_REGION]:
        record = json.loads((STATE / f"{region}-provision.json").read_text())
        name = f"pe-{region}"
        endpoint = az(
            "network", "private-endpoint", "create", "-g", GROUP, "-n", name, "-l", region,
            "--vnet-name", record["vnet"], "--subnet", record["subnet"],
            "--private-connection-resource-id", scope, "--group-id", "blob",
            "--connection-name", f"{name}-connection",
            "--tags", "purpose=blob-locality-benchmark", f"run={RUN}",
        )
        nic = az("network", "nic", "show", "--ids", endpoint["networkInterfaces"][0]["id"])
        record["blob_private_ip"] = nic["ipConfigurations"][0]["privateIPAddress"]
        record["private_endpoint"] = name
        record["private_connection_status"] = endpoint["privateLinkServiceConnections"][0]["privateLinkServiceConnectionState"]["status"]
        save(f"{region}-provision.json", record)
        print(f"{region}: Blob private endpoint {record['blob_private_ip']}", flush=True)
    print("Provisioning finished", flush=True)


if __name__ == "__main__":
    main()
