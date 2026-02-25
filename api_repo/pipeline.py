import git
import papermill as pm

REPO_URL = "https://github.com/NaaVRE/vl-laserfarm.git"

git.Repo.clone_from(REPO_URL, "repo")

pm.execute_notebook(
    "repo/notebook.ipynb",
    "output.ipynb"
)