from fastapi import FastAPI, Body, HTTPException, status
from fastapi.responses import JSONResponse
from mangum import Mangum
from typing import Optional, Dict
import json
from aws-batch import start_job

app = FastAPI()

@app.get("/")
def root():
    return {"message": "post to /notebook"}

@app.post("/notebook/{notebook_name}")
def run_notebook(
    notebook_name: str,
    other_params: Optional[Dict] = Body(default=None)
):
    """
    args:
        notebook_name: name of the notebook to be deployed
        other_params: user-given parameters to be used instead
            of what is dumped in paramdump.json . paramdump
            contains defaults

    out:
        http response code:
            421 if notebook doesnt exist
            400 if parameters are misspelled
            202 if notebook successfully started execution
        container execution id (?) if successful
    """
    # print("works")
    dump = open("paramdump.json", "r", encoding="utf-8")
    all_params = json.load(dump)
    # print("works")
    if notebook_name not in all_params.keys():
        raise HTTPException(
        status_code=421,
        detail="notebook does not exist"
    )
    # print("works")
    params = all_params[notebook_name]
    if other_params:
        for p in other_params.keys():
            if p not in params.keys():
                raise HTTPException(
                    status_code=400,
                    detail=f"given param {p} does not exist"
                )
            else:
                params[p] = other_params[p]
    # TODO: handle container deployment
    valid_name = notebook_name.lower().rstrip(".ipynb")
    execution_id = 0
    try:
        execution_id = start_job(valid_name, params)
    except:
        raise HTTPException(
            status_code=500,
            detail="something went wrong while starting job"
        )
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"execution_id": execution_id}
    )


handler = Mangum(app)

