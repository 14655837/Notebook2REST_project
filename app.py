import json
from typing import Any, Dict, Optional

from aws_batch import *
from fastapi import Body, FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse, Response
from mangum import Mangum
from pydantic import BaseModel

# ============================================================================
# Pydantic Models
# ============================================================================


class JobResponse(BaseModel):
    id: str
    notebook: str
    status: str


class JobListResponse(BaseModel):
    items: list[JobResponse]


class NotebookSummary(BaseModel):
    notebook: str


class NotebookListResponse(BaseModel):
    items: list[NotebookSummary]


class NotebookDetail(BaseModel):
    notebook: str
    params: Dict[str, Any]


class ErrorResponse(BaseModel):
    error: str
    detail: str


# ============================================================================
# Custom Exceptions
# ============================================================================


class NotebookNotFoundError(Exception):
    def __init__(self, notebook: str):
        self.notebook = notebook
        self.message = f"No notebook named '{notebook}'. Use GET /jobs/notebooks to see available notebooks."


class InvalidParamsError(Exception):
    def __init__(self, invalid: list[str], valid: list[str]):
        self.invalid = invalid
        self.valid = valid


class JobNotFoundError(Exception):
    def __init__(self, job_id: str):
        self.job_id = job_id


class OutputNotReadyError(Exception):
    def __init__(self, job_id: str, job_status: str):
        self.job_id = job_id
        self.job_status = job_status


class InternalServerError(Exception):
    def __init__(self, detail: str):
        self.detail = detail


# ============================================================================
# FastAPI App
# ============================================================================


app = FastAPI(
    title="Notebook2REST",
    description="REST API for executing Jupyter notebooks on AWS Batch",
    version="2.0",
)


# ============================================================================
# Exception Handlers
# ============================================================================


@app.exception_handler(NotebookNotFoundError)
async def notebook_not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={
            "error": "notebook_not_found",
            "detail": exc.message,
        },
    )


@app.exception_handler(InvalidParamsError)
async def invalid_params_handler(request, exc):
    return JSONResponse(
        status_code=422,
        content={
            "error": "invalid_params",
            "detail": "Unknown parameters provided.",
            "invalid": exc.invalid,
            "valid": exc.valid,
        },
    )


@app.exception_handler(JobNotFoundError)
async def job_not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={
            "error": "job_not_found",
            "detail": f"No job found with id '{exc.job_id}'",
        },
    )


@app.exception_handler(OutputNotReadyError)
async def output_not_ready_handler(request, exc):
    return JSONResponse(
        status_code=409,
        content={
            "error": "output_not_ready",
            "detail": "Job exists but has not produced output yet.",
            "job_id": exc.job_id,
            "status": exc.job_status,
        },
    )


@app.exception_handler(InternalServerError)
async def internal_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "error": "internal",
            "detail": exc.detail,
        },
    )


# ============================================================================
# Reusable Error Response Documentation for OpenAPI
# ============================================================================

RESP_NOTEBOOK_NOT_FOUND = {
    404: {"model": ErrorResponse, "description": "Notebook not found"}
}
RESP_JOB_NOT_FOUND = {404: {"model": ErrorResponse, "description": "Job not found"}}
RESP_OUTPUT_NOT_READY = {
    409: {
        "model": ErrorResponse,
        "description": "Job exists but output is not ready yet",
    }
}
RESP_INVALID_PARAMS = {
    422: {"model": ErrorResponse, "description": "Invalid parameters provided"}
}
RESP_INVALID_STATUS = {
    422: {"model": ErrorResponse, "description": "Invalid status filter value"}
}
RESP_INTERNAL = {500: {"model": ErrorResponse, "description": "Internal server error"}}


# ============================================================================
# Helper Functions
# ============================================================================


def load_paramdump() -> dict:
    """Load and return the contents of paramdump.json.

    Raises:
        InternalServerError: If paramdump.json cannot be loaded.
    """
    try:
        with open("paramdump.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        raise InternalServerError("Could not load notebook definitions.")


# ============================================================================
# Routes (CRITICAL: Order matters. Do not reorder.)
# ============================================================================


@app.post(
    "/jobs/{notebook}",
    status_code=202,
    response_model=JobResponse,
    responses={**RESP_NOTEBOOK_NOT_FOUND, **RESP_INVALID_PARAMS, **RESP_INTERNAL},
)
def create_job(
    notebook: str,
    param_overrides: Optional[Dict[str, Any]] = Body(default=None),
):
    """
    Create a new job to execute a notebook on AWS Batch.

    Path parameter:
    - notebook: notebook name (without .ipynb)

    Optional request body:
    {
      "param_var_x": 10,
      "param_var_y": 20
    }
    """
    paramdump = load_paramdump()

    # Validate notebook name
    notebook_with_ext = f"{notebook}.ipynb"
    if notebook_with_ext not in paramdump:
        raise NotebookNotFoundError(notebook)

    # Load defaults for this notebook
    params = paramdump[notebook_with_ext].copy()

    # Validate and merge user-provided parameters
    if param_overrides:
        invalid_keys = [k for k in param_overrides.keys() if k not in params]
        if invalid_keys:
            raise InvalidParamsError(invalid_keys, list(params.keys()))
        params.update(param_overrides)

    # Submit the job
    try:
        job_id = start_job(notebook, params)
    except Exception as e:
        raise InternalServerError(f"Failed to submit job: {str(e)}")

    job_response = JobResponse(id=job_id, notebook=notebook, status="queued")
    return JSONResponse(
        status_code=202,
        content=job_response.model_dump(),
        headers={"Location": f"/jobs/{job_id}"},
    )


@app.get(
    "/jobs",
    status_code=200,
    response_model=JobListResponse,
    responses={**RESP_INVALID_STATUS, **RESP_INTERNAL},
)
def list_jobs(
    status_filter: Optional[str] = Query(None, alias="status")
) -> JobListResponse:
    """
    List jobs, optionally filtered by status.

    Query parameters:
    - status: One of "queued", "running", "succeeded", "failed" (optional)
    """
    try:
        jobs = list_all_jobs(status_filter=status_filter)
    except ValueError as e:
        raise HTTPException(
            status_code=422,
            detail=f"Must be one of: {', '.join(VALID_API_STATUSES)}.",
        )
    except Exception as e:
        raise InternalServerError(f"Failed to list jobs: {str(e)}")

    return JobListResponse(items=[JobResponse(**job) for job in jobs])


@app.get(
    "/jobs/notebooks",
    status_code=200,
    response_model=NotebookListResponse,
    responses={**RESP_INTERNAL},
)
def list_notebooks() -> NotebookListResponse:
    """
    List all available notebooks (names only).
    """
    paramdump = load_paramdump()

    # Strip .ipynb extension
    items = [
        NotebookSummary(notebook=name.removesuffix(".ipynb"))
        for name in sorted(paramdump.keys())
    ]

    return NotebookListResponse(items=items)


@app.get(
    "/jobs/notebooks/{name}",
    status_code=200,
    response_model=NotebookDetail,
    responses={**RESP_NOTEBOOK_NOT_FOUND, **RESP_INTERNAL},
)
def get_notebook_detail(name: str) -> NotebookDetail:
    """
    Get full detail for a notebook: its name and all accepted parameters with defaults.
    """
    paramdump = load_paramdump()

    notebook_with_ext = f"{name}.ipynb"
    if notebook_with_ext not in paramdump:
        raise NotebookNotFoundError(name)

    return NotebookDetail(notebook=name, params=paramdump[notebook_with_ext])


@app.get(
    "/jobs/{job_id}",
    status_code=200,
    response_model=JobResponse,
    responses={**RESP_JOB_NOT_FOUND, **RESP_INTERNAL},
)
def get_job_detail(job_id: str) -> JobResponse:
    """
    Get the status and details of a single job.
    """
    try:
        job = get_job(job_id)
    except ValueError:
        raise JobNotFoundError(job_id)
    except Exception as e:
        raise InternalServerError(f"Failed to retrieve job: {str(e)}")

    return JobResponse(**job)


@app.get(
    "/jobs/{job_id}/output",
    response_model=None,
    responses={**RESP_JOB_NOT_FOUND, **RESP_OUTPUT_NOT_READY, **RESP_INTERNAL},
)
def download_job_output(job_id: str):
    """
    Download the executed output notebook file for a job.
    """
    # First, check if the job exists
    try:
        job_detail = get_job(job_id)
    except ValueError:
        raise JobNotFoundError(job_id)
    except Exception as e:
        raise InternalServerError(f"Failed to retrieve job: {str(e)}")

    # If job exists but output is not ready, return 409
    if job_detail["status"] != "succeeded":
        raise OutputNotReadyError(job_id, job_detail["status"])

    # Try to fetch the output
    try:
        notebook_bytes = get_job_output(job_id)
    except ValueError as e:
        # Job claims it succeeded but output is missing
        raise InternalServerError(str(e))
    except ClientError as e:
        raise InternalServerError(f"AWS error: {str(e)}")
    except Exception as e:
        raise InternalServerError(f"Failed to retrieve output: {str(e)}")

    # Return the notebook file
    headers = {"Content-Disposition": f'attachment; filename="{job_id}.ipynb"'}
    return Response(
        content=notebook_bytes,
        media_type="application/x-ipynb+json",
        headers=headers,
    )


# ============================================================================
# Lambda Handler
# ============================================================================

handler = Mangum(app)
