"""Execute a regional notebook over an IPC-only local Jupyter kernel."""

import os
from pathlib import Path

import nbformat
from jupyter_client import KernelManager
from nbclient import NotebookClient

from provision import CONFIG


region = os.environ["BENCH_REGION"]
source = Path(__file__).resolve().with_name("blob-locality.ipynb")
notebook = nbformat.read(source, as_version=4)
kernel = KernelManager(
    kernel_name="python3",
    transport="ipc",
    connection_file=f"/home/bench/kernel-{region}.json",
)
client = NotebookClient(
    notebook, km=kernel, timeout=3600,
    resources={"metadata": {"path": str(source.parent)}},
)
try:
    client.execute()
finally:
    nbformat.write(notebook, source.with_name(f"blob-locality-{region}-executed.ipynb"))
for cell in notebook.cells:
    for output in cell.get("outputs", []):
        if output.output_type == "stream":
            print(output.text, flush=True)
print(f"Completed regional notebook: {region}", flush=True)
