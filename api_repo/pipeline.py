# pipeline.py
import ast
import glob
import json
import os
import shutil
from typing import Any, Dict

import git
import nbformat

REPO_URL = "https://github.com/14655837/test_repo_for_notebook"
# REPO_URL = "https://github.com/NaaVRE/vl-laserfarm"
REPO_DIR = "repo"
REPO_ROOT = ".." 
NOTEBOOK_OUTPUT_DIR = os.path.join(REPO_ROOT, "notebooks")

os.makedirs(NOTEBOOK_OUTPUT_DIR, exist_ok=True)


def clone_repo():
    """Clone the repo if not exists, else pull latest."""
    if os.path.exists(REPO_DIR):
        shutil.rmtree(REPO_DIR)
    git.Repo.clone_from(REPO_URL, REPO_DIR)
    print(f"Repo cloned to {REPO_DIR}")


def find_notebooks() -> list[str]:
    notebooks = glob.glob(f"{REPO_DIR}/**/*.ipynb", recursive=True)
    if not notebooks:
        raise FileNotFoundError("No notebooks found in the repository")
    return notebooks


def add_notebook_to_github(notebook_path: str, parameters: Dict[str, Any] = None) -> str:
    """Executes a notebook with optional parameters, returns output path."""
    notebook_name = os.path.basename(notebook_path).replace(".ipynb", "")
    output_path = os.path.join(NOTEBOOK_OUTPUT_DIR, f"{notebook_name}.ipynb")

    return output_path


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
                                if isinstance(
                                    target, ast.Name
                                ) and target.id.startswith("param_"):
                                    try:
                                        # Safely evaluate the value (strings, numbers, lists, dicts)
                                        value = ast.literal_eval(node.value)
                                        extracted_params[target.id] = value
                                    except (ValueError, SyntaxError):
                                        # If it's a complex expression, we label it or skip it
                                        extracted_params[target.id] = (
                                            "Expression/Non-Literal"
                                        )
                        elif isinstance(node, ast.AnnAssign):
                            # Handle annotated assignments: param_x: int = 5
                            if (
                                isinstance(node.target, ast.Name)
                                and node.target.id.startswith("param_")
                                and node.value is not None
                            ):
                                try:
                                    value = ast.literal_eval(node.value)
                                    extracted_params[node.target.id] = value
                                except (ValueError, SyntaxError):
                                    extracted_params[node.target.id] = (
                                        "Expression/Non-Literal"
                                    )
                except SyntaxError:
                    # Skip cells that have invalid Python syntax
                    continue

        return extracted_params

    except Exception as e:
        return {"error": f"Failed to parse notebook: {str(e)}"}


def main():
    clone_repo()
    notebooks = find_notebooks()
    all_params = {}
    for nb in notebooks:
        add_notebook_to_github(nb)
        params = get_param_variables_json(nb)
        all_params[nb.split("/")[-1]] = params
    with open("paramdump.json", "w") as f:
        json.dump(all_params, f, indent=4)


main()
