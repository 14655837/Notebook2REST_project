import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import nbformat


NOTEBOOK_IN = "/app/notebook.ipynb"


def load_payload() -> dict:
    if os.getenv("JOB_JSON"):
        return json.loads(os.environ["JOB_JSON"])
    job_file = os.getenv("JOB_JSON_FILE", "/app/job.json")
    return json.loads(Path(job_file).read_text(encoding="utf-8"))


def build_param_cell(params: dict) -> str:
    lines = ["# Auto-generated parameters. Do not edit."]
    for k, v in params.items():
        if not k.startswith("param_") or not k.isidentifier():
            raise ValueError(f"Invalid parameter name: {k}")
        lines.append(f"{k} = {repr(v)}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    payload = load_payload()

    notebook_out = payload.get("notebook_out")
    if not notebook_out:
        raise SystemExit("Missing 'notebook_out' in JSON payload.")

    # Accept either nested "params" or flat param_* keys
    params = payload.get("params")
    if params is None:
        params = {k: v for k, v in payload.items() if k.startswith("param_")}
    else:
        params = {k: v for k, v in params.items() if k.startswith("param_")}

    if not params:
        raise SystemExit("No param_* values found in payload.")

    nb = nbformat.read(NOTEBOOK_IN, as_version=4)

    # Replace first code cell
    for cell in nb.cells:
        if cell.get("cell_type") == "code":
            cell.source = build_param_cell(params)
            break
    else:
        raise SystemExit("Notebook contains no code cells.")

    with tempfile.TemporaryDirectory() as td:
        patched = Path(td) / "patched.ipynb"
        nbformat.write(nb, str(patched))

        cmd = ["papermill", str(patched), notebook_out]
        print("Running:", " ".join(cmd), flush=True)
        return subprocess.call(cmd)


if __name__ == "__main__":
    sys.exit(main())