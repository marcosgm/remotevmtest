# Remote GPU vs Local GPU

Compare the cost and completion time of compute beside Blob Storage (Region 1)
with compute in another region (Region 2): **stage once, process on local SSD,
publish once**.

The included experiment used matching **CPU VMs**, not GPUs. The report is a
GPU-placement **what-if calculator calibrated with CPU-pipeline measurements**,
not evidence of GPU training performance. Changing calculator inputs does not
run Azure jobs or modify the measured evidence.

## 1. Open the report without creating Azure resources

Use Linux or WSL with Python 3.12+ and `python3-venv`. Run commands from the
project directory:

```bash
python3 serve_report.py
```

Open <http://127.0.0.1:18764/customer-report.html>. With VS Code Remote SSH or WSL,
forward port **18764** in the Ports view if necessary. The server binds to
loopback and serves only the report; use this instead of a restricted `file://`
preview. Stop the foreground server with Ctrl+C. If the port already has this
server running, reuse it: it reads the latest HTML on every request.

[customer-report.html](./customer-report.html) is standalone and can be sent as
an attachment and opened in a normal browser with JavaScript enabled. Rebuilding
it requires Python dependencies plus the matching [experiment.json](./experiment.json),
[manifest.json](./manifest.json), and recorded metrics, ingestion ledger and
pricing under `evidence/<run>/`. Raw deployment evidence and executed notebooks
are deliberately excluded from Git; they are not included in a fresh clone.
Retain them privately when copying a completed experiment. To rebuild after
collecting your own matching evidence:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python build_report.py
```

If you only have the HTML, no Python or evidence files are needed to view it.

### Calculator inputs

- **Egress:** USD per decimal GB transferred between regions, applied once at
  the source to inputs and the small returning result.
- **Local/remote VM prices:** independent USD/hour prices, charged throughout
  staging, processing and publication. Replace the illustrative defaults with
  your GPU quotes.
- **Number of input files:** downloadable **Blob objects**, not files inside a
  tar archive. The baseline has **5 Blob files containing 421,587 logical files**.
- **Total file size:** combined compressed input size in decimal GB
  (`1 GB = 1,000,000,000 bytes`). The baseline is **1.165321470 GB**.
- **Additional processing:** extra hours per job, assumed equal in both
  locations. This is not automatically multiplied by dataset size.
- **Allocation settings:** jobs/month, input retention and dedicated versus
  shared private endpoint hours.

The measured bandwidth and time to first byte stay visible and **do not change**
when you edit inputs. They describe the original experiment:

| Median measurement | Local placement | Remote placement |
|---|---:|---:|
| Aggregate staging bandwidth | 110.78 MiB/s | 31.22 MiB/s |
| Time to first byte | 146 ms | 751 ms |
| Stage all inputs | 10.03 s | 35.60 s |

Bandwidth includes writing/fsync to local disk and SHA-256 validation, not just
wire speed. TTFB includes connection setup on fresh connections. Staging is the
median across three complete jobs per placement; TTFB summarizes 15 GETs per
placement.

### How workload projections work

The model preserves the original measurements exactly at the original size and
file count. For different workloads:

```text
request waves = ceil(file count / measured download workers)
baseline payload time = measured stage time - baseline waves * measured TTFB
projected stage time = baseline payload time * byte-size ratio
                       + new request waves * measured TTFB
projected local work = baseline local work * byte-size ratio + additional hours
```

The experiment uses four workers. Separating request latency from payload time
avoids counting baseline TTFB twice. Whole-job time retains the measured
whole-job residual; the small output size and upload duration stay fixed.

Retrieval, capacity, Private Link processing and egress scale with bytes.
GET transaction counts scale with Blob-file count. The original ingest ledger
is used at the original workload; otherwise ingest requests assume equally
sized files, 16-MiB blocks and one block-list commit per file.

These are **extrapolations**, especially for many tiny files. Effective
bandwidth, file-size distribution, request scheduling, CPU work, local disk
capacity and GPU speed may differ. The calculator does not validate whether a
dataset fits on the selected VM's SSD. It does not model distributed training,
repeated remote streaming or large recurring checkpoints.

Cost estimates include Cold retrieval, read/write transactions, shared input
capacity/ingestion, small result capacity, Private Link and time-allocated
OS-disk/public-IP costs. Cold capacity has a 90-day minimum retention charge,
including early deletion. Initial upload writes use a conservative all-Cold
estimate. Provisioning/idle time, unmeasured disk operations, diagnostic traffic,
taxes and contract-specific adjustments are excluded. This is not an invoice.

## 2. Run the tests

Python dependencies are in [requirements.txt](./requirements.txt). Node.js 18+
is also needed for the report's JavaScript-model tests; no npm packages are used.
Install dependencies into the virtual environment before running tests:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m unittest -v test_benchmark
```

The command above tests HTTP instrumentation, parameterized discovery and
notebook generation without private evidence. The **full suite** additionally
requires matching local evidence from a completed experiment:

```bash
.venv/bin/python -m unittest -v test_benchmark test_report
```

Neither command creates Azure resources. Run the full suite before replacing
a completed experiment's configuration/evidence, and again after your own
experiment has produced its matching evidence.

## 3. Rerun the real Azure experiment

**This section creates billable resources.** Work in a separate project copy
for a fresh run: the configuration, manifest, notebooks and report are rewritten.
The included experiment's resources have been deleted. Do not reuse its account,
subscription configuration, resource group or SSH credentials.

### Prerequisites and scope

- Azure CLI (`az`), `ssh`, `scp`, `ssh-keygen`, `curl`, and the local environment above.
- Your own Azure subscription and permission to provision Compute, Network and
  Storage resources, create scoped role assignments and delete the test group.
  Owner is sufficient; a suitably scoped combination of roles may also work.
- Two regions with an actually deployable matching VM size, sufficient regional
  and VM-family quota, local temporary SSD, and approved private connectivity.
- Policy approval for the topology and any cross-region movement. Do not relax
  organizational policy or expose the storage account publicly to make it work.
- Sufficient local disk space for the source corpus and outputs. The VM's
  temporary SSD must mount at `/mnt`.

The runner uses the region and SKU parameters in
[experiment.json](./experiment.json) throughout discovery, provisioning,
notebook placement checks, retail pricing, reporting and cleanup cost estimates:

- `storage_region`: Blob Storage and the local VM's Azure region code.
- `remote_region`: the second VM's region code; choose a different region.
- `vm_size`: one Azure VM SKU used for **both** VMs.

No Python edits are needed to change these selections. Use lowercase Azure
region codes, not display names. Optional `storage_region_name` and
`remote_region_name` fields are not needed.

The SKU must support the pinned Ubuntu x64 image, Trusted Launch, accelerated
networking and a temporary SSD mounted at `/mnt`; sizes without local temporary
storage are not suitable. The included selection is `Standard_D2ds_v5`.
Selecting a compatible GPU SKU changes hardware and pricing, **not** the
workload: it still runs the same CPU pipeline without GPU training or drivers.
Actual GPU claims require a GPU workload and separate timing validation.
The corpus remains five Linux archives, three trials and three local fits;
the size/file inputs change the **report model**, not the uploaded corpus.

### Configure and prepare locally

1. Sign in, select your subscription, and edit [experiment.json](./experiment.json).
   Use a fresh lowercase alphanumeric `run` suffix, at most 14 characters:
   the storage account is named `stlocality<run>` and must be globally unique.
   The resource group will be `rg-blob-locality-<run>`.

   ```bash
   az login --use-device-code
   az account set --subscription "<your-subscription-id>"
   ```

   ```json
   {
     "run": "mybench01",
     "subscription": "<your-subscription-id>",
     "storage_region": "<region-1-code>",
     "remote_region": "<region-2-code>",
     "vm_size": "Standard_D2ds_v5",
     "trials": 3,
     "epochs": 3,
     "bandwidth_usd_per_decimal_gb": 0.02,
     "bandwidth_scope": "<verified transfer-pricing scope for your pair>"
   }
   ```

   Replace all placeholders. Verify the bandwidth rate for your pair: $0.02/GB
   is not universal. The report's editable defaults remain an illustrative
   $0.02/GB and a 10% cheaper remote VM; they are not current GPU retail quotes.
   For example, `storage_region: eastus2`, `remote_region: centralus` and
   `vm_size: Standard_D4ds_v5` selects two matching VMs with storage in the
   first region; this example is not a capacity guarantee. Use a fresh `run`
   for any changed selection, and regenerate/copy the notebook before execution.
   Do not relabel or overwrite the supplied historical measurements.

2. Verify required provider registrations in your subscription. Register only
   the missing providers, subject to your permissions:

   ```bash
   az provider register --namespace Microsoft.Compute --wait
   az provider register --namespace Microsoft.Network --wait
   az provider register --namespace Microsoft.Storage --wait
   .venv/bin/python discover_regions.py
   ```

   [discover_regions.py](./discover_regions.py) reads your configured
   subscription and defaults to your configured region pair and SKU. To
   search a wider set, pass explicit candidates:

   ```bash
   .venv/bin/python discover_regions.py \
     --regions eastus2 centralus westus2 \
     --vm-sizes Standard_D2ds_v5 Standard_D4ds_v5
   ```

   These flags affect discovery only; save your selected pair and single SKU
   in [experiment.json](./experiment.json) before the remaining commands.
   Discovery writes SKU/restriction evidence but does not deploy or guarantee
   capacity. Have your assistant also check quotas, temporary storage, image
   compatibility and zone restrictions. A zone-only restriction does not
   necessarily block a nonzonal deployment.

3. Before starting VMs, prepare the fixed public corpus and fetch prices:

   ```bash
   .venv/bin/python ingest.py --download-only
   .venv/bin/python fetch_prices.py
   .venv/bin/python make_notebook.py
   ```

   This downloads about 1.165 GB from kernel.org, checks its HTTPS checksum
   list, and records the manifest. It does not verify OpenPGP signatures.
   Pricing queries use the selected SKU in each region and require a unique
   Linux PAYG hourly rate; Windows, Spot, Low Priority and reservation offers
   are not substituted. Missing or ambiguous prices stop the workflow.

### Provision, connect and copy inputs

```bash
.venv/bin/python provision.py
```

[provision.py](./provision.py) creates only the dedicated test group: identical
VMs, private containers, managed identities with account-scoped Blob permission,
and one approved Blob private endpoint per VM VNet. Public storage access and
shared-key access stay disabled. SSH is restricted to the controller's current
public IPv4 `/32`.

The dedicated SSH key defaults to `.benchmark-ssh/id_ed25519`, excluded by
[.gitignore](./.gitignore). Optionally set `BENCH_SSH_KEY` to a dedicated absolute
key path **before** provisioning and keep the same setting throughout the run.
Never commit or share the private key. The unencrypted generated key is only for
this short-lived experiment.

Use the following bootstrap from the project directory after provisioning.
Verify host fingerprints through your approved Azure administration channel;
`accept-new` is trust-on-first-use and never accepts a changed known key.

```python
import json
import shlex
import subprocess
from provision import ACCOUNT, LOCAL_REGION, REMOTE_REGION, ROOT, STATE
from run_regions import SSH_OPTIONS, host

bootstrap_options = [
    "StrictHostKeyChecking=accept-new" if x == "StrictHostKeyChecking=yes" else x
    for x in SSH_OPTIONS
]
files = [
    "benchmark.py", "provision.py", "experiment.json", "ingest.py",
    "blob-locality.ipynb", "execute_notebook.py", "manifest.json",
]
for region in [LOCAL_REGION, REMOTE_REGION]:
    record = json.loads((STATE / f"{region}-provision.json").read_text())
    entry = shlex.quote(f"{record['blob_private_ip']} {ACCOUNT}.blob.core.windows.net")
    command = (
        "sudo cloud-init status --wait && "
        "sudo install -d -o bench -g bench /mnt/locality/source && "
        "test -x /home/bench/venv/bin/python && "
        "findmnt -T /mnt/locality && "
        f"printf '%s\\n' {entry} | sudo tee -a /etc/hosts"
    )
    subprocess.run(["ssh", *bootstrap_options, host(region), command], check=True)
    subprocess.run(["scp", *SSH_OPTIONS, *files, host(region) + ":/home/bench/"], check=True)

manifest = json.loads((ROOT / "manifest.json").read_text())
subprocess.run([
    "scp", *SSH_OPTIONS,
    *[str(ROOT / ".dataset" / item["name"]) for item in manifest],
    host(LOCAL_REGION) + ":/mnt/locality/source/",
], check=True)
```

Run this with the project's `.venv/bin/python` interpreter, for example through
your assistant's terminal. The `/etc/hosts` mapping is a short-lived PoC DNS
setup; use approved Private DNS for production. TLS still uses the normal Blob
hostname. Check that `findmnt` reports the temporary SSD at `/mnt`, not `/`.

### Ingest, execute and preserve evidence

```bash
.venv/bin/python run_regions.py ingest
.venv/bin/python make_notebook.py
```

Ingestion runs on the storage-local VM using managed identity. It uploads
16-MiB blocks, commits each object as Cold, and retrieves the manifest and API
ledger to your controller. After regenerating the notebook with the uploaded
manifest, copy it to both VMs:

```python
import subprocess
from provision import LOCAL_REGION, REMOTE_REGION
from run_regions import SSH_OPTIONS, host

for region in [LOCAL_REGION, REMOTE_REGION]:
    subprocess.run([
        "scp", *SSH_OPTIONS, "blob-locality.ipynb", host(region) + ":/home/bench/",
    ], check=True)
```

Then run:

```bash
.venv/bin/python run_regions.py measure
.venv/bin/python build_report.py
.venv/bin/python -m unittest -v test_benchmark test_report
```

[run_regions.py](./run_regions.py) executes both notebooks concurrently and
collects `evidence/<run>/<region>-metrics.json` and one executed notebook per
region. Each of three jobs does five fresh input GETs, all local processing,
and one final result PUT: **30 GETs + 6 PUTs** across both locations. Ingestion
requests are logged separately. Do not transfer the corpus or run competing
workloads during measurement. Do not blindly rerun failed measurements over
existing Cold result names: overwrites can incur minimum-retention charges.

### Clean up immediately after verifying outputs

Review the group, tags and collected outputs first:

```bash
.venv/bin/python cleanup_experiment.py
```

[cleanup_experiment.py](./cleanup_experiment.py) verifies both complete
notebooks, expected group tags and VM inventory before deleting the dedicated
group, then checks that the group is absent and saves cleanup evidence.
Cold minimum-retention charges still apply.

If provisioning or measurement failed before complete evidence exists, this
cleanup helper deliberately refuses to proceed. Have your assistant inspect
the **exact dedicated group** and obtain your confirmation before deleting
only that group with Azure CLI. Do not leave failed/idle resources running.
Once the group is verified absent, remove only its dedicated key files and
known-hosts file; never delete a shared SSH directory.

## 4. A prompt for your AI coding assistant

Open this directory in VS Code, share this README, and paste:

> Help me reproduce this project using my own Azure subscription. First inspect
> the scripts and included evidence without deploying anything. Explain the
> CPU-proxy versus actual-GPU distinction and prepare a phased plan and cost
> estimate. Ask for my subscription, approved regions and a fresh run suffix.
> Verify matching deployable SKUs, quotas, local SSD and private-network policy.
> Use experiment.json as the configuration source, never the previous owner's
> subscription or machine paths. Keep the existing results intact in another
> copy. Get my approval before creating billable resources or changing scope.
>
> Follow the README setup, bootstrap, ingestion, paired measurement and report
> commands. Keep public Blob access disabled, use managed identity and normal
> TLS validation, and do not bypass policy or reopen security rules removed by
> governance. Do not print tokens or private keys. Pause on authentication or
> network errors, preserve the failed attempt, and explain the cause.
>
> Collect real bytes, TTFB, bandwidth, complete-job timings and every Blob HTTP
> attempt. Separate measured results from extrapolations and do not force a
> conclusion favoring either placement. Use the configuration parameters for
> region and SKU selection. If I request actual GPU training or another
> dataset, update the processing, metrics and cleanup assumptions before
> execution. Verify tests and the report in a
> browser. Preserve evidence, delete only the dedicated tagged test group,
> independently confirm deletion, and summarize actual costs and limitations.

## Troubleshooting and useful files

- **`SkuNotAvailable`:** use discovery and quota evidence, then select an
  approved matching pair; quota availability alone is not capacity.
- **Blob 403:** check scoped managed-identity RBAC propagation, approved
  private endpoint status and hostname resolution. Do not enable public access.
- **SSH stops working:** check your source IPv4, NSG and policy first. Do not
  restore policy-removed rules automatically. If authorized, the existing
  `run_regions.py recover` exports the **remote** completed artifacts via the
  Azure VM agent with whole-file hash verification. Adapting that helper for
  another VM requires an explicit reviewed change; never rerun the benchmark
  merely to collect an already completed result.
- **Report rebuild fails:** ensure config/run, manifest, uploaded flags,
  ingestion ledger, pricing and both regional metrics belong to the same run.
  Projection inputs cannot substitute for missing measurement evidence.
- [benchmark.py](./benchmark.py): explicit HTTP instrumentation and local work.
- [make_notebook.py](./make_notebook.py): embeds benchmark code and manifest.
- [execute_notebook.py](./execute_notebook.py): executes through a local IPC kernel.
- [fetch_prices.py](./fetch_prices.py): public retail meters; bandwidth comes
  from your configuration and must be independently verified.
- [build_report.py](./build_report.py): validates evidence and generates the
  generic, self-contained report without embedding named deployment regions.

Raw evidence and configuration may contain subscription/resource identifiers.
Review them before sharing. Sharing the generic report does not require sharing
credentials, private keys or infrastructure identifiers.
The committed subscription is a placeholder. Any `experiment.private.json`
backup, executed notebooks, raw evidence and local editor settings stay ignored;
never force-add them to a public repository.
