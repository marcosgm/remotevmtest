"""Find a common temporary-SSD VM SKU in alternative Azure regions."""

import argparse
import concurrent.futures
import datetime as dt
import json
import pathlib
import subprocess

import requests

ROOT = pathlib.Path(__file__).resolve().parent
CONFIG = json.loads((ROOT / "experiment.json").read_text())
SUBSCRIPTION = CONFIG["subscription"]
REGIONS = [CONFIG["storage_region"], CONFIG["remote_region"]]
CANDIDATES = [CONFIG["vm_size"]]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--regions", nargs="+", default=REGIONS,
                        help="Candidate region codes; defaults to the configured pair.")
    parser.add_argument("--vm-sizes", nargs="+", default=CANDIDATES,
                        help="Candidate Azure VM SKUs; defaults to the configured vm_size.")
    args = parser.parse_args(argv)
    regions = list(dict.fromkeys(args.regions))
    candidates = list(dict.fromkeys(args.vm_sizes))
    token = json.loads(subprocess.check_output(
        ["az", "account", "get-access-token", "--subscription", SUBSCRIPTION, "-o", "json"],
        text=True,
    ))["accessToken"]
    directory = ROOT / "evidence" / "region-discovery"
    directory.mkdir(parents=True, exist_ok=True)

    def region_skus(region):
        url = (
            f"https://management.azure.com/subscriptions/{SUBSCRIPTION}"
            "/providers/Microsoft.Compute/skus"
        )
        params = {"api-version": "2021-07-01", "$filter": f"location eq '{region}'"}
        items = []
        while url:
            response = requests.get(
                url, params=params, headers={"Authorization": f"Bearer {token}"}, timeout=180,
            )
            response.raise_for_status()
            payload = response.json()
            items.extend(x for x in payload["value"] if x["resourceType"] == "virtualMachines")
            url, params = payload.get("nextLink"), None
        eligible = [
            x for x in items
            if not any(r["type"] == "Location" for r in x.get("restrictions", []))
        ]
        result = {
            "region": region, "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "vm_skus": len(items), "location_eligible": len(eligible),
            "candidates": [x for x in eligible if x["name"] in candidates],
        }
        (directory / f"{region}.json").write_text(json.dumps(result, indent=2) + "\n")
        print(
            region, f"eligible {len(eligible)}/{len(items)}",
            [x["name"] for x in result["candidates"]], flush=True,
        )
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(region_skus, regions))
    for candidate in candidates:
        matches = [r["region"] for r in results if any(x["name"] == candidate for x in r["candidates"])]
        if len(matches) >= 2:
            print("MATCH", candidate, matches, flush=True)
    (directory / "summary.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
