"""Persist exact public USD PAYG retail meters, with provenance and no inferred discounts."""

import concurrent.futures
import datetime as dt
import json
import math
import pathlib

import requests

API = "https://prices.azure.com/api/retail/prices"
ROOT = pathlib.Path(__file__).resolve().parent
CONFIG = json.loads((ROOT / "experiment.json").read_text())


def vm_query(region, vm_size):
    return (
        f"serviceName eq 'Virtual Machines' and armRegionName eq '{region}' "
        f"and armSkuName eq '{vm_size}' and priceType eq 'Consumption'"
    )


def vm_hourly_price(items, vm_size, region):
    values = {
        item["retailPrice"] for item in items
        if item.get("armSkuName") == vm_size and item.get("armRegionName") == region
        and item.get("type") == "Consumption" and item.get("unitOfMeasure") == "1 Hour"
        and item.get("isPrimaryMeterRegion") is True
        and item.get("productName", "").startswith("Virtual Machines ")
        and item["productName"].endswith(" Series")
        and not any(term in (item.get("skuName", "") + " " + item["meterName"]).lower()
                    for term in ["spot", "low priority", "lowpriority"])
    }
    if len(values) != 1:
        raise RuntimeError(f"Missing/ambiguous Linux PAYG hourly VM price: {region}/{vm_size}")
    value = values.pop()
    if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise RuntimeError(f"Invalid VM price: {region}/{vm_size}")
    return value


def fetch(label, query):
    url = API
    params = {"$filter": query, "currencyCode": "USD"}
    items = []
    urls = []
    while url:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        urls.append(response.url)
        payload = response.json()
        items.extend(payload["Items"])
        url, params = payload["NextPageLink"], None
    return label, {"urls": urls, "items": items}


def main():
    queries = {}
    for region in [CONFIG["storage_region"], CONFIG["remote_region"]]:
        queries[f"vm_{region}"] = vm_query(region, CONFIG["vm_size"])
        queries[f"disk_{region}"] = (
            f"serviceName eq 'Storage' and armRegionName eq '{region}' "
            "and productName eq 'Standard HDD Managed Disks' and skuName eq 'S4 LRS'"
        )
        queries[f"ip_{region}"] = (
            f"serviceName eq 'Virtual Network' and armRegionName eq '{region}' "
            "and productName eq 'IP Addresses' and skuName eq 'Standard' "
            "and meterName eq 'Standard IPv4 Static Public IP' and priceType eq 'Consumption'"
        )
    queries["blob_storage_region"] = (
        f"serviceName eq 'Storage' and armRegionName eq '{CONFIG['storage_region']}' "
        "and productName eq 'General Block Blob v2' and skuName eq 'Cold LRS'"
    )
    queries["blob_hot_write_storage_region"] = (
        f"serviceName eq 'Storage' and armRegionName eq '{CONFIG['storage_region']}' "
        "and productName eq 'General Block Blob v2' and skuName eq 'Hot LRS' "
        "and (meterName eq 'Hot LRS Write Operations' or meterName eq 'All Other Operations')"
    )
    queries["private_link_global"] = (
        "armRegionName eq 'Global' and productName eq 'Virtual Network Private Link' "
        "and skuName eq 'Standard' and priceType eq 'Consumption' "
        "and tierMinimumUnits eq 0"
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results = dict(pool.map(lambda p: fetch(*p), queries.items()))
    data = {
        "retrieved_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "currency": "USD", "basis": "public retail PAYG, not an invoice or contract quote",
        "queries": results,
        "bandwidth": {
            "source": "https://azure.microsoft.com/en-us/pricing/details/bandwidth/",
            "usd_per_decimal_GB": CONFIG["bandwidth_usd_per_decimal_gb"],
            "scope": CONFIG["bandwidth_scope"],
            "note": "Applied to outbound payload each direction; incoming is free. Storage GB are GiB, bandwidth GB are decimal. Private Link processing is additional.",
        },
    }
    for label, value in results.items():
        if not value["items"]:
            raise RuntimeError(f"No pricing meters found: {label}")
        print(label, [(x["meterName"], x["retailPrice"], x["unitOfMeasure"]) for x in value["items"]])
    for region in [CONFIG["storage_region"], CONFIG["remote_region"]]:
        vm_hourly_price(results[f"vm_{region}"]["items"], CONFIG["vm_size"], region)
    directory = ROOT / "evidence" / CONFIG["run"]
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "pricing.json").write_text(json.dumps(data, indent=2) + "\n")


if __name__ == "__main__":
    main()
