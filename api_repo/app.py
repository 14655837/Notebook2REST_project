from fastapi import FastAPI
import subprocess

app = FastAPI()

@app.post("/run")
def run_notebook():
    subprocess.run(["python", "pipeline.py"], check=True)
    return {"status": "done"}