"""Build a generic, standalone GPU locality cost/time scenario from measured timing."""

import collections
import html
import json
import math
import pathlib
import statistics

from fetch_prices import vm_hourly_price

ROOT = pathlib.Path(__file__).resolve().parent
CONFIG = json.loads((ROOT / "experiment.json").read_text())
STATE = ROOT / "evidence" / CONFIG["run"]
REGIONS = [CONFIG["storage_region"], CONFIG["remote_region"]]


def load(path):
    return json.loads(path.read_text())


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def table(headers, rows):
    return (
        '<div class="scroll"><table><thead><tr>'
        + "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
        + "</tr></thead><tbody>"
        + "".join("<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in row) + "</tr>" for row in rows)
        + "</tbody></table></div>"
    )


def verify(metrics, manifest, region):
    size = sum(item["bytes"] for item in manifest)
    require(metrics["measurement_type"] == "regional_vm", "Offline data cannot be a regional result")
    env = metrics["environment"]
    require(env["azure_vm"]["location"].lower() == region, f"Wrong region: {region}")
    require(env["azure_vm"]["vmSize"] == CONFIG["vm_size"], "VM sizes differ")
    require(json.loads(env["mount"])["filesystems"][0]["target"] == "/mnt", "Not on temporary SSD")
    require(len(metrics["trials"]) == CONFIG["trials"], "Incomplete trial set")
    expected_counts = {"GetBlob": len(manifest) * CONFIG["trials"], "PutBlob": CONFIG["trials"]}
    counts = dict(collections.Counter(event["operation"] for event in metrics["operations"]))
    require(counts == metrics["api_call_counts"] == expected_counts, "Unexpected Blob operation counts")
    for event in metrics["operations"]:
        require(200 <= event["status"] < 300 and "error" not in event, "Failed Blob operation")
        require(event["server_request_id"], "Missing server request ID")
        if event["operation"] == "GetBlob":
            require(event["tier"] == "Cold", "Input is not Cold")
    for trial in metrics["trials"]:
        require(trial["stage"]["bytes"] == size, "Incomplete input payload")
        require(sum(e["response_body_bytes"] for e in trial["stage"]["blob_events"]) == size, "Byte count mismatch")
        require(len(trial["epochs"]) == CONFIG["epochs"], "Incomplete local fits")
        require(trial["local_blob_calls"] == 0, "Local computation accessed Blob")
        require(trial["upload"]["calls"] == 1, "More than one result upload")
        require(trial["preprocess"]["files"] > 400000, "Corpus not fully processed")


def report_data():
    manifest = load(ROOT / "manifest.json")
    size = sum(x["bytes"] for x in manifest)
    require(1e9 <= size <= 2e9, "Dataset outside requested size")
    require(all(x["uploaded"] for x in manifest), "Inputs were not uploaded")
    metrics = [load(STATE / f"{region}-metrics.json") for region in REGIONS]
    for region, item in zip(REGIONS, metrics):
        verify(item, manifest, region)
    worker_counts = {t["stage"]["workers"] for item in metrics for t in item["trials"]}
    require(len(worker_counts) == 1, "Download concurrency differs across trials")
    workers = worker_counts.pop()
    require(isinstance(workers, int) and workers > 0, "Invalid download concurrency")
    reference = metrics[0]["trials"][0]["preprocess"]
    for item in metrics:
        for trial in item["trials"]:
            require(all(
                trial["preprocess"][key] == reference[key]
                for key in ["files", "uncompressed_bytes", "letters", "lines", "features_bytes"]
            ), "Pipeline outputs differ")
    ingest = load(STATE / "ingest.json")
    require(all(200 <= x["status"] < 300 and "error" not in x for x in ingest["operations"]), "Ingest failure")
    counts = dict(collections.Counter(x["operation"] for x in ingest["operations"]))
    require(counts == {
        "PutBlock": sum(math.ceil(x["bytes"] / (16 * 2**20)) for x in manifest),
        "PutBlockList": len(manifest),
    }, "Unexpected ingestion API counts")
    queries = load(STATE / "pricing.json")["queries"]

    def rate(query, name):
        values = {x["retailPrice"] for x in queries[query]["items"] if x["meterName"] == name}
        require(len(values) == 1, f"Missing/ambiguous meter: {query}/{name}")
        return values.pop()

    rates = {
        "diskMonth": [rate(f"disk_{r}", "S4 LRS Disk") for r in REGIONS],
        "ipHour": [rate(f"ip_{r}", "Standard IPv4 Static Public IP") for r in REGIONS],
        "storedGiBMonth": rate("blob_storage_region", "Cold LRS Data Stored"),
        "retrievalGiB": rate("blob_storage_region", "Cold LRS Data Retrieval"),
        "reads10k": rate("blob_storage_region", "Cold LRS Read Operations"),
        "writes10k": rate("blob_storage_region", "Cold LRS Write Operations"),
        "endpointHour": rate("private_link_global", "Standard Private Endpoint"),
        "linkInboundGB": rate("private_link_global", "Standard Data Processed - Ingress"),
        "linkOutboundGB": rate("private_link_global", "Standard Data Processed - Egress"),
    }
    profiles = []
    for label, item in zip(["Local GPU", "Remote GPU"], metrics):
        trials = item["trials"]
        downloads = [e for e in item["operations"] if e["operation"] == "GetBlob"]
        profiles.append({
            "name": label,
            "stage": statistics.median(t["stage"]["seconds"] for t in trials),
            "local": statistics.median(
                t["preprocess"]["seconds"] + sum(e["seconds"] for e in t["epochs"]) for t in trials
            ),
            "upload": statistics.median(t["upload"]["seconds"] for t in trials),
            "total": statistics.median(t["total_seconds"] for t in trials),
            "outputBytes": statistics.median(t["upload"]["bytes"] for t in trials),
            "throughput": statistics.median(t["stage"]["MiB_s_including_hash_validation"] for t in trials),
            "ttfbMs": statistics.median(e["ttfb_body_seconds"] * 1000 for e in downloads),
        })
        require(profiles[-1]["stage"] > math.ceil(len(manifest) / workers) * profiles[-1]["ttfbMs"] / 1000,
                "Measured stage is too short to calibrate the transfer model")
    local_vm_price = vm_hourly_price(
        queries[f"vm_{REGIONS[0]}"]["items"], CONFIG["vm_size"], REGIONS[0],
    )
    return {
        "size": size, "files": reference["files"], "inputBlobs": len(manifest),
        "trials": CONFIG["trials"], "profiles": profiles, "rates": rates, "workers": workers,
        "ingestWrites": sum(counts.values()),
        "defaults": {"egress": 0.02, "localVM": local_vm_price, "remoteVM": round(local_vm_price * 0.9, 6)},
    }


def build_page(data):
    defaults = data["defaults"]
    profiles = data["profiles"]
    baseline = table(["Measured CPU proxy: median", "Local placement", "Remote placement"], [
        ["Stage inputs to local SSD", *[f"{p['stage']:.2f} s" for p in profiles]],
        ["Process locally", *[f"{p['local']:.2f} s" for p in profiles]],
        ["Publish final result", *[f"{p['upload']:.3f} s" for p in profiles]],
        ["Whole job", *[f"{p['total']:.2f} s" for p in profiles]],
    ])
    transfer_baseline = table(["Measured network metric", "Local placement", "Remote placement"], [
        ["Aggregate staging bandwidth", *[f"{p['throughput']:.2f} MiB/s" for p in profiles]],
        ["Time to first byte (TTFB)", *[f"{p['ttfbMs']:.0f} ms" for p in profiles]],
    ])
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Remote GPU vs Local GPU, considering cost and time to complete</title>
<style>
:root{{color-scheme:light;--ink:#17283a;--muted:#526376;--line:#d5dde5;--accent:#075b74}}
*{{box-sizing:border-box}}body{{margin:0;background:#f1f5f8;color:var(--ink);font:16px/1.6 system-ui,sans-serif}}
main{{max-width:1080px;margin:auto;padding:36px 28px 60px}}header{{padding:30px;background:#102e41;color:white;border-radius:12px}}
h1{{font-size:clamp(28px,4vw,42px);line-height:1.2;margin:8px 0 12px}}h2{{font-size:23px;margin:0 0 12px}}h3{{margin:0 0 8px}}
header p{{margin:0;color:#d4e5ef;font-size:19px}}section{{background:white;padding:26px;margin-top:22px;border:1px solid var(--line);border-radius:10px;min-width:0}}
.muted,small,figcaption{{color:var(--muted)}}small{{display:block;font-weight:400}}a{{color:var(--accent)}}p{{margin:10px 0}}
.architecture{{margin:20px auto 0;max-width:800px;text-align:center}}.storage-node{{position:relative;margin:0 auto;width:min(100%,360px);padding:16px;border:2px solid var(--accent);border-radius:10px;background:#eaf5f8}}
.storage-node::after{{content:"";position:absolute;top:100%;left:50%;height:32px;border-left:2px solid #7996a7}}
.branches{{position:relative;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));margin-top:30px}}
.branches::before{{content:"";position:absolute;top:0;left:25%;right:25%;border-top:2px solid #7996a7}}
.branch{{position:relative;padding:40px 12px 0;min-width:0}}.branch::before{{content:"";position:absolute;top:0;left:50%;height:38px;border-left:2px solid #7996a7}}
.branch::after{{content:"";position:absolute;top:31px;left:calc(50% - 4px);width:9px;height:9px;border-right:2px solid #7996a7;border-bottom:2px solid #7996a7;transform:rotate(45deg)}}
.vm-node{{padding:16px 10px;border:1px solid #aebfcb;border-radius:9px;background:#f7fafc}}.vm-node strong{{display:block;font-size:19px}}.vm-node span{{display:block;font-size:14px}}
.transfer-note{{font-size:14px;margin:8px 0 0}}figcaption{{font-size:14px;margin:15px 0 0}}.flow{{text-align:center;margin:20px 0 0;font-weight:600}}
.pricing-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}}label{{display:flex;flex-direction:column;gap:6px;min-width:0;font-weight:650;font-size:14px}}
input,select{{font:inherit;padding:10px;min-width:0;width:100%;border:1px solid #9db1c0;border-radius:5px;background:white;color:var(--ink)}}
.workload{{max-width:360px;margin-top:20px}}details{{margin-top:20px}}summary{{cursor:pointer;font-weight:650}}.advanced{{margin-top:14px}}
#calc-error{{color:#a12727}}.callout{{padding:12px 16px;background:#edf7fa;border-left:4px solid var(--accent)}}
.result-cards{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;margin:18px 0}}
.result-card{{border:1px solid var(--line);border-radius:9px;padding:20px;min-width:0}}.result-card h3{{font-size:18px}}.value{{font-size:30px;font-weight:750;margin:4px 0;overflow-wrap:anywhere;font-variant-numeric:tabular-nums}}
.time{{font-weight:650;margin-top:12px}}.scroll{{max-width:100%;overflow:auto;margin-top:14px}}table{{border-collapse:collapse;width:100%;font-size:14px;font-variant-numeric:tabular-nums}}
th,td{{border-bottom:1px solid var(--line);padding:10px;text-align:left;overflow-wrap:anywhere;min-width:105px}}th{{background:#edf3f7}}tr.total{{font-weight:750}}ul{{padding-left:22px}}
@media(max-width:640px){{main{{padding:16px 12px 40px}}header,section{{padding:20px}}.pricing-grid,.result-cards{{grid-template-columns:1fr}}.branch{{padding-inline:6px}}.vm-node strong{{font-size:16px}}.vm-node span,.transfer-note{{font-size:12px}}}}
@media print{{body{{background:white}}main{{padding:0;max-width:none}}header{{background:white;color:black;border:1px solid #aaa}}header p{{color:#526376}}section{{break-inside:avoid}}.scroll{{overflow:visible}}}}
</style></head><body><main>
<header><h1>Remote GPU vs Local GPU</h1><p>Considering cost and time to complete</p></header>

<section aria-labelledby="architecture-title"><h2 id="architecture-title">One dataset. Two compute locations.</h2>
<figure class="architecture" aria-label="Blob storage in Region 1 feeds either a local GPU in Region 1 or a remote GPU in Region 2">
<div class="storage-node"><strong>Blob Storage</strong><small>Region 1 &middot; shared input and final results</small></div>
<div class="branches">
<div class="branch"><div class="vm-node"><strong>Local GPU</strong><span>Region 1</span><span>VM-local SSD</span></div><p class="transfer-note">Same-region transfer</p></div>
<div class="branch"><div class="vm-node"><strong>Remote GPU</strong><span>Region 2</span><span>VM-local SSD</span></div><p class="transfer-note">Cross-region transfer + egress</p></div>
</div><figcaption>Choose either compute location. Each downloads the input once and publishes one small result back to Blob Storage.</figcaption>
</figure><p class="flow">Stage once &rarr; Process on local SSD &rarr; Publish once</p>
</section>

<section aria-labelledby="scenario-title"><h2 id="scenario-title">Set your scenario</h2>
<p class="muted">Enter your GPU VM quotes and transfer rate. Defaults illustrate $0.02/GB egress and a remote VM
priced 10% below the local VM; they are not GPU price quotes.</p>
<form id="scenario" novalidate>
<div class="pricing-grid">
<label for="egress-cost">Egress cost (USD/GB)<input id="egress-cost" type="number" min="0" step="any" value="{defaults['egress']}"><small>Applied to cross-region input and result payloads.</small></label>
<label for="local-vm-cost">Region 1 &middot; Local VM (USD/hour)<input id="local-vm-cost" type="number" min="0" step="any" value="{defaults['localVM']}"><small>Charged during staging, processing and upload.</small></label>
<label for="remote-vm-cost">Region 2 &middot; Remote VM (USD/hour)<input id="remote-vm-cost" type="number" min="0" step="any" value="{defaults['remoteVM']}"><small>Independent of the local VM price.</small></label>
</div>
<div class="pricing-grid advanced">
<label for="file-count">Number of input files (Blob objects)<input id="file-count" type="number" min="1" step="1" value="{data['inputBlobs']}"><small>Count downloadable objects, not files packed inside archives.</small></label>
<label for="total-size">Total file size (GB)<input id="total-size" type="number" min="0.000000001" step="any" value="{data['size'] / 1e9}"><small>Combined compressed input size; 1 GB = 1,000,000,000 bytes.</small></label>
</div>
<label class="workload" for="extra">Additional processing (hours/job)<input id="extra" type="number" min="0" step="any" value="0"><small>Added equally to both observed baselines. Use this to model longer local training.</small></label>
<details><summary>Allocation settings</summary><div class="pricing-grid advanced">
<label for="jobs">Jobs per 30-day month<input id="jobs" type="number" min="1" step="1" value="30"></label>
<label for="days">Input retention (days)<input id="days" type="number" min="1" step="1" value="90"></label>
<label for="endpoint">Private endpoint allocation<select id="endpoint"><option value="ephemeral">Dedicated: round up job hours</option><option value="retained">Retained: 720 hours / monthly jobs</option></select></label>
</div></details></form>
<p class="muted">File count and size update transfer costs, Blob requests and estimated completion time.
The size-based model scales baseline processing; additional processing hours remain your independent input.</p>
<p class="callout"><strong>Timing basis:</strong> measured on CPU VMs, not GPUs. This is a placement and cost model,
not measured GPU training performance. Added processing time assumes equal duration in both locations;
changing a price does not predict a change in hardware speed.</p>
</section>

<section aria-labelledby="measured-network-title"><h2 id="measured-network-title">Measured transfer baseline</h2>
<div id="measured-network">{transfer_baseline}</div>
<p class="muted">Actual experiment: {data['inputBlobs']} packed Blob files totaling {data['size'] / 1e9:.6f} GB,
containing {data['files']:,} logical files; {data['workers']} concurrent downloads and {data['trials']} jobs per placement.
These measured values stay fixed when you edit the scenario.</p>
<p class="muted">Bandwidth is total payload divided by staging wall time, including local writes and hash checks.
TTFB includes connection setup and is the median across all {data['inputBlobs'] * data['trials']} input GETs per placement.</p>
</section>

<section aria-labelledby="results-title"><h2 id="results-title">Cost and time to complete</h2>
<p id="calc-error" role="alert"></p><noscript>Enable JavaScript to calculate costs from your inputs.</noscript>
<div id="calculator" aria-live="polite"></div>
<p class="muted">Estimated USD per job, not an invoice. VM runtime includes staging and final upload; the additional
processing duration is modeled, not measured.</p>
</section>

<section aria-labelledby="interpretation-title"><h2 id="interpretation-title">What matters</h2>
<p>Moving compute away from storage adds a one-time transfer cost and staging delay. A lower GPU hourly price can
offset that premium when enough processing reuses the local data. Repeated remote reads or large frequent
checkpoints can change the outcome.</p>
<details><summary>Measured baseline and assumptions</summary>
<p>The timing baseline is the median of {data['trials']} complete jobs per location on matching CPU VM sizes
(host CPUs can differ). Each job downloaded all five packed archives, checked their hashes, processed
{data['files']:,} logical files on local temporary SSD, fitted a small filename-size model three times,
and uploaded one small result. It made five input GETs and one result PUT; processing made zero Blob calls.</p>
{baseline}
<p>Staging includes local disk writes, fsync and SHA-256 checks with {data['workers']} parallel downloads. TTFB includes connection
setup and is the median across all input GETs. Whole-job time is a separately measured median; it is not the sum of
individual stage medians. Repeated local work may use the OS page cache. These timings do not predict another GPU
VM's network, disk or training performance.</p>
<p><strong>Workload projections:</strong> changing file count or size is a what-if estimate, not a new experiment.
The transfer model separates the baseline stage into a payload component and
ceil(files / {data['workers']}) request waves at the measured median TTFB. It scales only the payload component
by the size ratio, then adds the new request waves, so baseline TTFB is not counted twice.
Baseline processing scales linearly with bytes; additional processing hours do not. The original whole-job timing
residual and small final result remain fixed. File size distribution, tiny-object overhead, service limits,
local SSD capacity and GPU throughput can invalidate these extrapolations.</p>
<p>Cost includes the entered VM prices and egress, Cold input retrieval and transactions, shared input storage and
ingestion, small result storage, private endpoint hours and payload processing, plus time-allocated OS disk and
public IP. Ingress is free; egress is counted once at the source, not at both ends. Storage uses binary GiB;
transfer estimates use decimal GB.</p>
<p>Defaults allocate shared input costs across 30 jobs/month over 90 days. Cold capacity has a 90-day minimum
retention charge, including early deletion; each result also incurs that minimum. Initial upload writes are
conservatively priced at Cold rates. Dedicated endpoint hours round up per job; retained endpoint hours are
allocated across monthly jobs. Disk allocation uses a 730-hour month.</p>
<p>At the original workload the ingestion ledger is used exactly. Other workloads assume equally sized input files
and estimate one block-list commit per file plus 16-MiB block uploads. Payload storage, retrieval and transfer
scale with total bytes; input GET transaction costs scale with file count. Result size stays fixed.</p>
<p>Shared service rates are held fixed while you vary the three pricing inputs. Private Link processing is based on
application payload rather than an invoice meter. Provisioning, idle VM time, unmeasured OS-disk operations,
diagnostic traffic, taxes and contract-specific adjustments are excluded. Staging and checkpoint behavior,
data residency and actual GPU speed must be validated for the target workload.</p>
<p>Pricing references: <a href="https://azure.microsoft.com/en-us/pricing/details/bandwidth/">Bandwidth</a>,
<a href="https://azure.microsoft.com/en-us/pricing/details/private-link/">Private Link</a>,
<a href="https://azure.microsoft.com/en-us/pricing/details/storage/blobs/">Blob Storage</a>.</p>
</details></section>
</main><script>
'use strict';
const C={json.dumps(data)};
const usd=(x,digits=4)=>'$'+x.toLocaleString('en-US',{{minimumFractionDigits:digits,maximumFractionDigits:digits}});
const duration=s=>s>=3600?(s/3600).toFixed(2)+' h':s>=60?(s/60).toFixed(2)+' min':s.toFixed(2)+' s';
function scenarioCosts(v) {{
 const r=C.rates,n=Math.max(1,v.jobs*v.days/30);
 const scale=v.bytes/C.size,baselineWaves=Math.ceil(C.inputBlobs/C.workers),waves=Math.ceil(v.files/C.workers);
 const inputCapacity=v.bytes/2**30*r.storedGiBMonth*Math.max(90,v.days)/30/n;
 const original=v.bytes===C.size&&v.files===C.inputBlobs;
 const writes=original?C.ingestWrites:v.files*(Math.ceil(v.bytes/v.files/(16*2**20))+1);
 const ingestion=(writes*r.writes10k/10000+v.bytes/1e9*r.linkOutboundGB)/n;
 return C.profiles.map((profile,i)=>{{
  const ttfb=profile.ttfbMs/1000;
  const stage=(profile.stage-baselineWaves*ttfb)*scale+waves*ttfb;
  const localSeconds=profile.local*scale+v.extra*3600;
  const seconds=profile.total+(stage-profile.stage)+(localSeconds-profile.local),hours=seconds/3600;
  const vm=hours*v.prices[i],diskIP=hours*(r.ipHour[i]+r.diskMonth[i]/730);
  const endpoint=v.endpoint==='retained'?720*r.endpointHour/v.jobs:Math.ceil(hours)*r.endpointHour;
  const link=v.bytes/1e9*r.linkInboundGB+profile.outputBytes/1e9*r.linkOutboundGB;
  const retrieval=v.bytes/2**30*r.retrievalGiB;
  const requests=(v.files*r.reads10k+r.writes10k)/10000;
  const egress=i?(v.bytes+profile.outputBytes)/1e9*v.egress:0;
  const capacity=inputCapacity+profile.outputBytes/2**30*r.storedGiBMonth*3;
  const total=vm+diskIP+endpoint+link+retrieval+requests+egress+capacity+ingestion;
  return {{seconds,stage,localSeconds,vm,diskIP,endpoint,link,retrieval,requests,egress,capacity,ingestion,total,monthly:total*v.jobs}};
 }});
}}
function calculate() {{
 const ids=['egress-cost','local-vm-cost','remote-vm-cost','extra','jobs','days','file-count','total-size'];
 const fields=ids.map(id=>document.getElementById(id)),values=fields.map(e=>Number(e.value));
 const error=document.querySelector('#calc-error'),target=document.querySelector('#calculator');
 if(fields.some(e=>e.value.trim()==='')||values.some(v=>!Number.isFinite(v)||v<0)||
    !Number.isInteger(values[4])||values[4]<1||!Number.isInteger(values[5])||values[5]<1||
    !Number.isSafeInteger(values[6])||values[6]<1||values[7]<=0) {{
  error.textContent='Enter whole positive file/job/retention counts, a positive total size, and nonnegative prices and processing hours.';
  target.replaceChildren();return;
 }}
 const [egress,localVM,remoteVM,extra,jobs,days,files,sizeGB]=values,bytes=Math.round(sizeGB*1e9);
 if(!Number.isSafeInteger(bytes)||bytes<1) {{
  error.textContent='Total size must be at least one byte and small enough to represent exactly.';target.replaceChildren();return;
 }}
 const v={{egress,prices:[localVM,remoteVM],extra,jobs,days,files,bytes,endpoint:document.querySelector('#endpoint').value}};
 const costs=scenarioCosts(v);
 if(!Number.isFinite(jobs*days)||!Number.isFinite(extra*3600)||
    costs.some(c=>Object.values(c).some(x=>!Number.isFinite(x)))) {{
  error.textContent='Values are too large to calculate reliably.';target.replaceChildren();return;
 }}
 error.textContent='';
 const delta=costs[1].total-costs[0].total,delay=costs[1].seconds-costs[0].seconds;
 const costText=Math.abs(delta)<1e-10?'costs the same per job':
  'costs '+usd(Math.abs(delta))+' '+(delta<0?'less':'more')+' per job ('+(Math.abs(delta)/costs[0].total*100).toFixed(1)+'%)';
 const timeText=Math.abs(delay)<1e-10?'takes the same time':
  'takes '+duration(Math.abs(delay))+' '+(delay<0?'less':'longer')+' ('+(Math.abs(delay)/costs[0].seconds*100).toFixed(1)+'%)';
 const cards=C.profiles.map((p,i)=>'<article class="result-card"><h3>'+p.name+'</h3><small>Region '+(i+1)+
  '</small><p class="value" id="'+(i?'remote-total':'local-total')+'">'+usd(costs[i].total)+
  '</p><small>estimated cost per job</small><p class="time" id="'+(i?'remote-time':'local-time')+'">'+
  duration(costs[i].seconds)+' to complete</p><small>'+usd(costs[i].monthly)+' / 30-day month</small></article>').join('');
 const rows=[
  ['VM runtime',...costs.map(c=>usd(c.vm,6))],
  ['Inter-region egress',...costs.map(c=>usd(c.egress,6))],
  ['Cold retrieval + Blob requests',...costs.map(c=>usd(c.retrieval+c.requests,6))],
  ['Private endpoint + payload processing',...costs.map(c=>usd(c.endpoint+c.link,6))],
  ['Allocated storage + initial ingestion',...costs.map(c=>usd(c.capacity+c.ingestion,6))],
  ['OS disk + public IP allocation',...costs.map(c=>usd(c.diskIP,6))],
  ['Total estimated cost per job',...costs.map(c=>usd(c.total,6))]
 ];
 const costTable='<div class="scroll"><table><thead><tr><th>Per-job cost</th><th>Local GPU</th><th>Remote GPU</th>'+
  '</tr></thead><tbody>'+rows.map((row,i)=>'<tr'+(i===rows.length-1?' class="total"':'')+'>'+
  row.map(cell=>'<td>'+cell+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>';
 const timingRows=[
  ['Stage inputs',...costs.map(c=>duration(c.stage))],
  ['Process locally',...costs.map(c=>duration(c.localSeconds))],
  ['Publish result',...C.profiles.map(p=>duration(p.upload))],
  ['Whole job',...costs.map(c=>duration(c.seconds))]
 ];
 const timeTable='<div class="scroll"><table><thead><tr><th>Time to complete</th><th>Local GPU</th><th>Remote GPU</th>'+
  '</tr></thead><tbody>'+timingRows.map(row=>'<tr>'+row.map(cell=>'<td>'+cell+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>';
 const projected=bytes!==C.size||files!==C.inputBlobs;
 const workloadNote='<p id="workload-summary">'+files.toLocaleString('en-US')+' input files; '+(bytes/1e9).toLocaleString('en-US',{{maximumFractionDigits:9}})+
  ' GB per job. '+(projected?'Timing is extrapolated from the fixed measured baseline.':'Original measured input workload; added processing, if any, is modeled.')+'</p>';
 target.innerHTML=workloadNote+'<p class="callout" id="comparison-summary">With these inputs, the remote GPU '+costText+
  ' and '+timeText+'.</p><div class="result-cards">'+cards+'</div>'+timeTable+
  '<details><summary>Cost breakdown</summary>'+costTable+'</details>';
}}
document.querySelector('#scenario').addEventListener('submit',event=>event.preventDefault());
document.querySelector('#scenario').addEventListener('input',calculate);
document.querySelector('#scenario').addEventListener('change',calculate);
calculate();
</script></body></html>"""


def main():
    data = report_data()
    path = ROOT / "customer-report.html"
    path.write_text(build_page(data))
    print(json.dumps({"report": str(path), "defaults": data["defaults"], "baseline": data["profiles"]}, indent=2))


if __name__ == "__main__":
    main()
