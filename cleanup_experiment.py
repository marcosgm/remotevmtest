"""Delete only the completed, tagged benchmark group, after preserving both notebooks."""

import datetime as dt
import json
import math
import subprocess

import nbformat

from build_report import REGIONS, ROOT, load, require, verify
from fetch_prices import vm_hourly_price
from provision import CONFIG, GROUP, STATE, SUBSCRIPTION, az, save


def main():
    manifest = load(ROOT / "manifest.json")
    regional = []
    for region in REGIONS:
        metrics = load(STATE / f"{region}-metrics.json")
        verify(metrics, manifest, region)
        regional.append(metrics)
        notebook = nbformat.read(ROOT / f"blob-locality-{region}-executed.ipynb", as_version=4)
        nbformat.validate(notebook)
        require(notebook.cells[3].source == (ROOT / "benchmark.py").read_text(), "Executed benchmark source differs")
        require(notebook.cells[-2].execution_count is not None, "Notebook did not run")
        require(not any(
            output.output_type == "error"
            for cell in notebook.cells if cell.cell_type == "code" for output in cell.outputs
        ), "Executed notebook contains errors")
    group = az("group", "show", "-n", GROUP)
    require(group["tags"] == {"purpose": "blob-locality-benchmark", "run": CONFIG["run"]},
            "Resource group tags differ from the expected experiment")
    inventory = az("resource", "list", "-g", GROUP)
    require({x["name"] for x in inventory if x["type"].lower() == "microsoft.compute/virtualmachines"}
            == {"vm-local", "vm-remote"}, "Unexpected compute resources in dedicated group")
    prices = load(STATE / "pricing.json")["queries"]
    vm_prices = {
        region: vm_hourly_price(prices[f"vm_{region}"]["items"], CONFIG["vm_size"], region)
        for region in REGIONS
    }
    started = dt.datetime.now(dt.timezone.utc)
    before = {
        "group": group, "inventory": inventory, "deletion_requested_at": started.isoformat(),
        "note": "All regional metrics and executed notebooks verified before deletion.",
    }
    save("before-cleanup.json", before)
    subprocess.run(
        ["az", "group", "delete", "-n", GROUP, "--subscription", SUBSCRIPTION,
         "--yes", "--only-show-errors"], check=True,
    )
    exists = az("group", "exists", "-n", GROUP)
    require(exists is False, "Resource group still exists")
    finished = dt.datetime.now(dt.timezone.utc)
    def price(query, name):
        values = {x["retailPrice"] for x in prices[query]["items"] if x["meterName"] == name}
        require(len(values) == 1, f"Ambiguous meter {query}/{name}")
        return values.pop()

    earliest = dt.datetime.fromisoformat(load(STATE / "resources.json")["created"])
    duration = (finished - earliest).total_seconds() / 3600
    costs = {}
    for region in REGIONS:
        provisioned = load(STATE / f"{region}-provision.json")
        hours = (finished - dt.datetime.fromisoformat(provisioned["started"])).total_seconds() / 3600
        costs[f"{region}: VM allocation, including setup/idle upper bound"] = hours * vm_prices[region]
        costs[f"{region}: OS disk time allocation upper bound"] = hours / 730 * price(f"disk_{region}", "S4 LRS Disk")
        costs[f"{region}: public IPv4 time allocation upper bound"] = hours * price(f"ip_{region}", "Standard IPv4 Static Public IP")
    costs["Two private endpoints, rounded whole-hour lifetime upper bound"] = (
        2 * math.ceil(duration) * price("private_link_global", "Standard Private Endpoint")
    )
    size = sum(x["bytes"] for x in manifest)
    outputs = sum(t["upload"]["bytes"] for m in regional for t in m["trials"])
    transactions = sum(len(m["operations"]) for m in regional)
    ingest = load(STATE / "ingest.json")
    costs["Cold corpus + six results, full 90-day minimum including early deletion"] = (
        (size + outputs) / 2**30 * price("blob_storage_region", "Cold LRS Data Stored") * 3
    )
    costs["Six complete Cold retrievals"] = 6 * size / 2**30 * price("blob_storage_region", "Cold LRS Data Retrieval")
    costs["30 notebook GET + 6 PUT transactions"] = (
        (transactions - 6) * price("blob_storage_region", "Cold LRS Read Operations")
        + 6 * price("blob_storage_region", "Cold LRS Write Operations")
    ) / 10000
    costs["Ingestion writes, conservative all-Cold bound"] = (
        len(ingest["operations"]) * price("blob_storage_region", "Cold LRS Write Operations") / 10000
    )
    remote_outputs = sum(t["upload"]["bytes"] for t in regional[1]["trials"])
    costs["Three cross-region input transfers + final result payloads"] = (
        (3 * size + remote_outputs) / 1e9 * CONFIG["bandwidth_usd_per_decimal_gb"]
    )
    costs["Private Link ingestion + all notebook payload processing"] = (
        (7 * size + outputs) / 1e9 * price("private_link_global", "Standard Data Processed - Ingress")
    )
    costs["TOTAL modeled allocation upper bound (not invoice)"] = sum(costs.values())
    save("cleanup.json", {
        "group": GROUP, "group_exists": exists, "verified_at": finished.isoformat(),
        "deletion_requested_at": started.isoformat(), "group_lifetime_hours": duration,
        "inventory": inventory, "estimated_cost_usd": costs,
        "cost_note": (
            "Lifetime estimate conservatively allocates VM/disk/IP charges from VM creation request to verified group "
            "deletion, plus two rounded endpoint lifetimes from resource-group creation, six full retrievals, "
            "recorded Blob requests, payload transfer and 90-day Cold capacity. Resources may have stopped billing "
            "earlier. This is an upper-bound allocation of these modeled items, not a bound on the invoice: "
            "OS-disk operations, setup/control traffic, artifact-export internet egress, offer-specific minimums "
            "and uninstrumented extension traffic are excluded. Both Private Link processing directions have the same rate."
        ),
    })
    print(json.dumps({"group_exists": exists, "estimated_cost_usd": costs}, indent=2))


if __name__ == "__main__":
    main()
