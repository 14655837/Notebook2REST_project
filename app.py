import json
from typing import Any, Dict, Optional

from aws_batch import *
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse, Response
from mangum import Mangum
from pydantic import BaseModel

app = FastAPI(
    title="Notebook2REST",
    description="REST API for executing Jupyter notebooks on AWS Batch",
    version="2.0",
)


# ============================================================================
# Pydantic Models
# ============================================================================


class JobCreateRequest(BaseModel):
    notebook: str
    params: Optional[Dict[str, Any]] = None


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


# ============================================================================
# Helper Functions
# ============================================================================


def error_response(status_code: int, error: str, detail: str, **extra) -> JSONResponse:
    """Build a consistent error response."""
    body = {"error": error, "detail": detail, **extra}
    return JSONResponse(status_code=status_code, content=body)


def load_paramdump() -> dict:
    """Load and return the contents of paramdump.json.

    Raises:
        FileNotFoundError: If paramdump.json does not exist.
        json.JSONDecodeError: If the file is not valid JSON.
    """
    with open("paramdump.json", "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================================
# Routes (CRITICAL: Order matters. Do not reorder.)
# ============================================================================


@app.post("/jobs")
def create_job(request: JobCreateRequest) -> JSONResponse:
    """
    Create a new job to execute a notebook on AWS Batch.

    Request body:
    {
      "notebook": "notebook",
      "params": { "param_var_x": 10, "param_var_y": 20 }
    }
    """
    try:
        paramdump = load_paramdump()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        return error_response(
            status_code=500,
            error="internal",
            detail="Could not load notebook definitions.",
        )

    # Validate notebook name
    notebook_with_ext = f"{request.notebook}.ipynb"
    if notebook_with_ext not in paramdump:
        return error_response(
            status_code=404,
            error="notebook_not_found",
            detail=f"No notebook named '{request.notebook}'. Use GET /jobs/notebooks to see available notebooks.",
        )

    # Load defaults for this notebook
    params = paramdump[notebook_with_ext].copy()

    # Validate and merge user-provided parameters
    if request.params:
        invalid_keys = [k for k in request.params.keys() if k not in params]
        if invalid_keys:
            return error_response(
                status_code=422,
                error="invalid_params",
                detail="Unknown parameters provided.",
                invalid=invalid_keys,
                valid=list(params.keys()),
            )
        params.update(request.params)

    # Submit the job
    try:
        job_id = start_job(request.notebook, params)
    except Exception as e:
        return error_response(
            status_code=500,
            error="internal",
            detail=f"Failed to submit job: {str(e)}",
        )

    response = JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "id": job_id,
            "notebook": request.notebook,
            "status": "queued",
        },
    )
    response.headers["Location"] = f"/jobs/{job_id}"
    return response


@app.get("/jobs")
def list_jobs(
    status_filter: Optional[str] = Query(None, alias="status")
) -> JSONResponse:
    """
    List jobs, optionally filtered by status.

    Query parameters:
    - status: One of "queued", "running", "succeeded", "failed" (optional)
    """
    try:
        jobs = list_all_jobs(status_filter=status_filter)
    except ValueError as e:
        return error_response(
            status_code=422,
            error="invalid_status",
            detail=f"Must be one of: {', '.join(VALID_API_STATUSES)}.",
            given=status_filter,
        )
    except Exception as e:
        return error_response(
            status_code=500,
            error="internal",
            detail=f"Failed to list jobs: {str(e)}",
        )

    return JSONResponse(
        status_code=200,
        content={"items": jobs},
    )


@app.get("/jobs/notebooks")
def list_notebooks() -> JSONResponse:
    """
    List all available notebooks (names only).
    """
    try:
        paramdump = load_paramdump()
    except (FileNotFoundError, json.JSONDecodeError):
        return error_response(
            status_code=500,
            error="internal",
            detail="Could not load notebook definitions.",
        )

    # Strip .ipynb extension
    items = [
        {"notebook": name.removesuffix(".ipynb")} for name in sorted(paramdump.keys())
    ]

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"items": items},
    )


@app.get("/jobs/notebooks/{name}")
def get_notebook_detail(name: str) -> JSONResponse:
    """
    Get full detail for a notebook: its name and all accepted parameters with defaults.
    """
    try:
        paramdump = load_paramdump()
    except (FileNotFoundError, json.JSONDecodeError):
        return error_response(
            status_code=500,
            error="internal",
            detail="Could not load notebook definitions.",
        )

    notebook_with_ext = f"{name}.ipynb"
    if notebook_with_ext not in paramdump:
        return error_response(
            status_code=404,
            error="notebook_not_found",
            detail=f"No notebook named '{name}'. Use GET /jobs/notebooks to see available notebooks.",
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "notebook": name,
            "params": paramdump[notebook_with_ext],
        },
    )


@app.get("/jobs/{job_id}")
def get_job_detail(job_id: str) -> JSONResponse:
    """
    Get the status and details of a single job.
    """
    try:
        job = get_job(job_id)
    except ValueError:
        return error_response(
            status_code=404,
            error="job_not_found",
            detail=f"No job found with id '{job_id}'",
        )
    except Exception as e:
        return error_response(
            status_code=500,
            error="internal",
            detail=f"Failed to retrieve job: {str(e)}",
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=job,
    )


@app.get("/jobs/{job_id}/output")
def download_job_output(job_id: str) -> Response | JSONResponse:
    """
    Download the executed output notebook file for a job.
    """
    # First, check if the job exists
    try:
        job_detail = get_job(job_id)
    except ValueError:
        return error_response(
            status_code=404,
            error="job_not_found",
            detail=f"No job found with id '{job_id}'",
        )
    except Exception as e:
        return error_response(
            status_code=500,
            error="internal",
            detail=f"Failed to retrieve job: {str(e)}",
        )

    # If job exists but output is not ready, return 409
    if job_detail["status"] != "succeeded":
        return error_response(
            status_code=409,
            error="output_not_ready",
            detail="Job exists but has not produced output yet.",
            job_id=job_id,
            status=job_detail["status"],
        )

    # Try to fetch the output
    try:
        notebook_bytes = get_job_output(job_id)
    except ValueError as e:
        # Job claims it succeeded but output is missing
        return error_response(
            status_code=500,
            error="internal",
            detail=str(e),
        )
    except ClientError as e:
        return error_response(
            status_code=500,
            error="internal",
            detail=f"AWS error: {str(e)}",
        )
    except Exception as e:
        return error_response(
            status_code=500,
            error="internal",
            detail=f"Failed to retrieve output: {str(e)}",
        )

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
