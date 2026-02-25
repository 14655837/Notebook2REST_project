# pipeline.py
import os
import shutil
import stat
import glob
import git
import papermill as pm
import nbformat
import ast
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, Any

#Set up URL and folders
#REPO_URL = "https://github.com/14655837/test_repo_for_notebook"
REPO_URL = "https://github.com/NaaVRE/vl-laserfarm"
REPO_DIR = "repo"
NOTEBOOK_OUTPUT_DIR = "outputs"

os.makedirs(NOTEBOOK_OUTPUT_DIR, exist_ok=True)

# -----------------------------
# Helper functions
# -----------------------------
def remove_readonly(func, path, excinfo):
    os.chmod(path, stat.S_IWRITE)
    func(path)

def clone_repo():
    """Clone the repo if not exists, else pull latest."""
    if os.path.exists(REPO_DIR):
        shutil.rmtree(REPO_DIR, onerror=remove_readonly)
    git.Repo.clone_from(REPO_URL, REPO_DIR)
    print(f"Repo cloned to {REPO_DIR}")

def find_notebooks() -> list[str]:
    notebooks = glob.glob(f"{REPO_DIR}/**/*.ipynb", recursive=True)
    if not notebooks:
        raise FileNotFoundError("No notebooks found in the repository")
    return notebooks

def execute_notebook(notebook_path: str, parameters: Dict[str, Any] = None) -> str:
    """Executes a notebook with optional parameters, returns output path."""
    notebook_name = os.path.basename(notebook_path).replace(".ipynb", "")
    output_path = os.path.join(NOTEBOOK_OUTPUT_DIR, f"{notebook_name}_output.ipynb")

    # pm.execute_notebook(
    #     notebook_path,
    #     output_path,
    #     parameters=parameters or {},
    #     kernel_name="python3"
    # )
    return output_path

def extract_notebook_outputs(output_path: str) -> dict:
    """Extract outputs from code cells as JSON."""
    nb = nbformat.read(output_path, as_version=4)
    results = []
    for cell in nb.cells:
        if cell.cell_type == "code" and cell.outputs:
            cell_out = []
            for out in cell.outputs:
                if out.output_type == "stream":
                    cell_out.append(out.text)
                elif out.output_type == "execute_result":
                    cell_out.append(out.data.get("text/plain", ""))
                elif out.output_type == "error":
                    cell_out.append({"error": out.evalue})
            results.append(cell_out)
    return {"cells": results}

def get_param_variables_json(output_path: str):
    """
    Find all the parameters, starting with 'param_', and put them in a json format
    """
    try:
        nb = nbformat.read(output_path, as_version=4)
        extracted_params = {}

        for cell in nb.cells:
            if cell.cell_type == "code":
                try:
                    # Put in AST
                    tree = ast.parse(cell.source)
                    
                    # Look for assignment nodes
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Assign):
                            for target in node.targets:
                                # Check if var starts with 'param_'
                                if isinstance(target, ast.Name) and target.id.startswith("param_"):
                                    try:
                                        # Safely evaluate the value (strings, numbers, lists, dicts)
                                        value = ast.literal_eval(node.value)
                                        extracted_params[target.id] = value
                                    except (ValueError, SyntaxError):
                                        # If it's a complex expression, we label it or skip it
                                        extracted_params[target.id] = "Expression/Non-Literal"
                except SyntaxError:
                    # Skip cells that have invalid Python syntax
                    continue

        return extracted_params

    except Exception as e:
        return {"error": f"Failed to parse notebook: {str(e)}"}

app = FastAPI(title="Notebook Runner API")

# Clone repo once at startup
clone_repo()
notebooks = find_notebooks()
print("Available notebooks:", notebooks)

class RunRequest(BaseModel):
    notebook_name: str
    parameters: Dict[str, Any] = {}

@app.get("/notebooks")
def list_notebooks():
    """List all notebooks in the repo."""
    return {"notebooks": [os.path.basename(nb) for nb in notebooks]}

@app.post("/run")
def run_notebook(req: RunRequest):
    notebook_path = next((nb for nb in notebooks if os.path.basename(nb) == req.notebook_name), None)
    if not notebook_path:
        raise HTTPException(status_code=404, detail="Notebook not found")
    output_path = execute_notebook(notebook_path, req.parameters)
    #outputs = extract_notebook_outputs(output_path)
    params = get_param_variables_json(output_path)
    return {"notebook": req.notebook_name, "params": params}
