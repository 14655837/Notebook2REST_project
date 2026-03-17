import ast
import glob
import json
import os
from typing import Any, Dict

import nbformat

# Search in current working directory
SEARCH_ROOT = os.getcwd()


def find_notebooks() -> list[str]:
    """Find all Jupyter notebooks in the current working directory."""
    notebooks = glob.glob(f"{SEARCH_ROOT}/**/*.ipynb", recursive=True)
    if not notebooks:
        raise FileNotFoundError("No notebooks found in the current directory")
    return notebooks


def get_param_variables_json(notebook_path: str) -> Dict[str, Any]:
    """
    Extract all variables starting with 'param_' from a notebook.
    """
    try:
        nb = nbformat.read(notebook_path, as_version=4)
        extracted_params = {}

        for cell in nb.cells:
            if cell.cell_type == "code":
                try:
                    tree = ast.parse(cell.source)

                    for node in ast.walk(tree):
                        # Handle standard assignments: param_x = ...
                        if isinstance(node, ast.Assign):
                            for target in node.targets:
                                if isinstance(target, ast.Name) and target.id.startswith("param_"):
                                    try:
                                        value = ast.literal_eval(node.value)
                                        extracted_params[target.id] = value
                                    except (ValueError, SyntaxError):
                                        extracted_params[target.id] = "Expression/Non-Literal"

                        # Handle annotated assignments: param_x: int = ...
                        elif isinstance(node, ast.AnnAssign):
                            if (
                                isinstance(node.target, ast.Name)
                                and node.target.id.startswith("param_")
                                and node.value is not None
                            ):
                                try:
                                    value = ast.literal_eval(node.value)
                                    extracted_params[node.target.id] = value
                                except (ValueError, SyntaxError):
                                    extracted_params[node.target.id] = "Expression/Non-Literal"

                except SyntaxError:
                    continue  # skip invalid cells

        return extracted_params

    except Exception as e:
        return {"error": f"Failed to parse notebook: {str(e)}"}


def main():
    notebooks = find_notebooks()
    all_params = {}

    for nb in notebooks:
        params = get_param_variables_json(nb)
        notebook_name = os.path.basename(nb)
        all_params[notebook_name] = params

    with open("paramdump.json", "w") as f:
        json.dump(all_params, f, indent=4)

    print(f"Processed {len(notebooks)} notebooks. Output saved to paramdump.json")


if __name__ == "__main__":
    main()
