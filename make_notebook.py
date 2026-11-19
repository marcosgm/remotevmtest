"""Generate a portable notebook; its code cell embeds the benchmark implementation."""

import json
import pathlib

import nbformat

ROOT = pathlib.Path(__file__).resolve().parent
CONFIG = json.loads((ROOT / "experiment.json").read_text())


def main():
    notebook = nbformat.v4.new_notebook()
    notebook.metadata.kernelspec = {
        "display_name": "Python 3", "language": "python", "name": "python3",
    }
    notebook.cells = [
        nbformat.v4.new_markdown_cell(
            "# Blob locality: stage once, process locally, publish once\n\n"
            f"Run unchanged on identical {CONFIG['vm_size']} VMs in "
            f"{CONFIG['storage_region']} and {CONFIG['remote_region']}. "
            f"The storage account is in {CONFIG['storage_region']}, targeting five Linux release archives "
            "explicitly stored in the **Cold** tier. Every logical source file is opened locally; "
            "letter/line counts and filename features feed a small ridge-regression model. "
            "This is a CPU data-pipeline proxy, **not a GPU performance benchmark**.\n\n"
            "Three independent jobs each stage all inputs afresh, process locally, train three "
            "local epochs and publish one small result. Every Blob HTTP attempt is recorded. "
            "No data listing or property calls are needed: the input manifest is embedded. "
            "The notebook aborts on invalid region, wrong VM size, non-Cold inputs, "
            "non-temporary work storage, hash mismatches or HTTP failures. "
            "Private Blob endpoints in each VM's VNet provide the same private-access pattern; "
            "the storage account does not allow public network access. The hosts entry on each VM "
            "maps the normal TLS Blob hostname to its own private endpoint. "
            "Do not interpret any earlier controller smoke test as a regional comparison."
        ),
        nbformat.v4.new_code_cell(
            "import os\n"
            "os.environ['OPENBLAS_NUM_THREADS'] = '1'\n"
            "os.environ['OMP_NUM_THREADS'] = '1'\n"
            f"ACCOUNT = os.environ.get('BENCH_ACCOUNT', 'stlocality{CONFIG['run']}')\n"
            f"REGION = os.environ.get('BENCH_REGION', '{CONFIG['storage_region']}')\n"
            f"VM_SIZE = os.environ.get('BENCH_VM_SIZE', {CONFIG['vm_size']!r})\n"
            "AUTH = os.environ.get('BENCH_AUTH', 'managed_identity')\n"
            "WORKDIR = os.environ.get('BENCH_WORKDIR', '/mnt/locality')\n"
            "TRIALS = int(os.environ.get('BENCH_TRIALS', '3'))\n"
            "EPOCHS = int(os.environ.get('BENCH_EPOCHS', '3'))\n"
            "OFFLINE = os.environ.get('BENCH_OFFLINE', '0') == '1'\n"
            "INPUT_DIRECTORY = os.environ.get('BENCH_INPUT_DIRECTORY', '.dataset')\n"
        ),
        nbformat.v4.new_markdown_cell(
            "## Instrumentation and local ML pipeline\n\n"
            "TTFB means time from HTTP request start through the first response-body byte "
            "(DNS/TCP/TLS included on a fresh connection). Per-Blob throughput includes "
            "writing/fsync to local disk; stage throughput additionally includes SHA-256 "
            "validation and is aggregate across four workers. Payload bytes exclude network "
            "headers and retransmissions. Interface counters include unrelated traffic and "
            "must not be treated as Blob billable bytes. Authentication is outside job timing.\n\n"
            "Local file repetitions may hit the OS page cache, as normal ML reuse does. "
            "No global caches are dropped. Tar members are streamed, never extracted onto "
            "arbitrary filesystem paths. SHA-256 is checked on every full download. "
            "Train/test grouping is deterministic by relative filename across release versions."
        ),
        nbformat.v4.new_code_cell((ROOT / "benchmark.py").read_text()),
        nbformat.v4.new_code_cell(
            "manifest = " + repr(json.loads((ROOT / "manifest.json").read_text())) + "\n"
            "manifest_path = pathlib.Path(WORKDIR) / 'manifest.json'\n"
            "manifest_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "manifest_path.write_text(json.dumps(manifest))\n"
            "if OFFLINE:\n"
            "    print('OFFLINE VALIDATION ONLY: no Azure data-plane benchmark or regional comparison')\n"
            "    metrics = run_local_validation(manifest_path, INPUT_DIRECTORY, WORKDIR, EPOCHS)\n"
            "    print(json.dumps({'measurement_type': metrics['measurement_type'], 'files': metrics['preprocess']['files'], 'blob_http_calls': 0}, indent=2))\n"
            "else:\n"
            "    metrics = run_experiment(ACCOUNT, manifest_path, WORKDIR, REGION, AUTH, TRIALS, EPOCHS,\n"
            "                             expected_vm_size=VM_SIZE)\n"
            "    print(json.dumps(metrics['api_call_counts'], indent=2))\n"
            "    assert all(t['local_blob_calls'] == 0 for t in metrics['trials'])\n"
        ),
        nbformat.v4.new_markdown_cell(
            "## Interpretation\n\n"
            "Compare median and range of stage, local preprocessing, local epochs, upload "
            "and full-job time separately. Inspect all per-Blob TTFBs and HTTP request IDs. "
            "Three runs characterize variability but do not establish statistical equivalence. "
            "Five large packed objects do not represent millions of small Blob GETs, BlobFuse "
            "random access, or per-epoch remote streaming. An otherwise identical GPU training "
            "job with longer local compute can amortize the staging penalty further; its actual "
            "GPU timing and GPU regional price must be measured independently."
        ),
    ]
    nbformat.validate(notebook)
    nbformat.write(notebook, ROOT / "blob-locality.ipynb")


if __name__ == "__main__":
    main()
