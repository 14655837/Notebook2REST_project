"""
This module provides functions for interacting with AWS Batch and S3 to execute Jupyter notebooks
as batch jobs. It exposes functions for job lifecycle management and output retrieval.

Key functions:
- `start_job`: Submits an AWS Batch job to execute a specified notebook with given parameters.
- `get_job`: Retrieves the status of a submitted batch job by UUID.
- `list_all_jobs`: Lists jobs across all statuses, with optional filtering.
- `get_job_output`: Fetches the executed notebook output from S3 once the job has completed.
"""

import json
import uuid

import boto3
from botocore.exceptions import ClientError

JOB_QUEUE = "Notebook2REST-fargate-job-queue"
JOBNAME_PREFIX = "Notebook2REST-"
S3_BUCKET = "notebook2rest"
NOTEBOOK_EXTENSION = ".ipynb"

# Status mapping: AWS Batch statuses → simplified API statuses
BATCH_STATUS_MAP = {
    "SUBMITTED": "queued",
    "PENDING": "queued",
    "RUNNABLE": "queued",
    "STARTING": "queued",
    "RUNNING": "running",
    "SUCCEEDED": "succeeded",
    "FAILED": "failed",
}

API_TO_BATCH_STATUSES = {
    "queued": ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING"],
    "running": ["RUNNING"],
    "succeeded": ["SUCCEEDED"],
    "failed": ["FAILED"],
}

ALL_BATCH_STATUSES = [
    "SUBMITTED",
    "PENDING",
    "RUNNABLE",
    "STARTING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
]

VALID_API_STATUSES = list(API_TO_BATCH_STATUSES.keys())


def map_status(batch_status: str) -> str:
    """Map AWS Batch status to simplified API status."""
    return BATCH_STATUS_MAP.get(batch_status, batch_status.lower())


def extract_job_uuid(job_name: str) -> str | None:
    """Extract our UUID from a Batch job name like 'Notebook2REST-<uuid>'."""
    if job_name.startswith(JOBNAME_PREFIX):
        return job_name[len(JOBNAME_PREFIX) :]
    return None


def start_job(notebook: str, params: dict) -> str:
    """
    Submits an AWS Batch job to execute the specified notebook with the given parameters.

    Args:
        notebook (str): The name of the notebook. Must match the job definition name
            registered at deploy time as Notebook2REST-<notebook>.
        params (dict): A dictionary of parameters to pass to the notebook container.
            Passed as a single JSON string environment variable to the container.

    Returns:
        str: A unique job ID (UUID) identifying the submitted batch job.
            This ID can be used to query job status via the Notebook2REST-<job_id> job name in AWS Batch.
    """

    job_id = str(uuid.uuid4())
    notebook_output_location = f"s3://{S3_BUCKET}/{job_id}{NOTEBOOK_EXTENSION}"

    boto3.client("batch").submit_job(
        jobName=f"{JOBNAME_PREFIX}{job_id}",
        jobQueue=JOB_QUEUE,
        jobDefinition=f"{JOBNAME_PREFIX}{notebook}",
        containerOverrides={
            "environment": [
                {"name": "NOTEBOOK_OUT", "value": notebook_output_location},
                {"name": "NOTEBOOK_PARAMS", "value": json.dumps(params)},
            ],
        },
        tags={
            "notebook": notebook,
        },
    )

    return job_id


def get_job(job_id: str) -> dict:
    """
    Look up a single job by its UUID and return its full details.

    Uses the JOB_NAME filter, which searches across all statuses regardless
    of the jobStatus parameter.

    Args:
        job_id (str): The UUID returned by start_job().

    Returns:
        dict: {"id": str, "notebook": str, "status": str (simplified)}

    Raises:
        ValueError: If no job is found with this ID.
    """
    batch = boto3.client("batch")
    job_name = f"{JOBNAME_PREFIX}{job_id}"

    response = batch.list_jobs(
        jobQueue=JOB_QUEUE,
        jobStatus="RUNNING",  # ignored when filters are used, but parameter is required
        filters=[{"name": "JOB_NAME", "values": [job_name]}],
    )

    jobs = response.get("jobSummaryList", [])
    if not jobs:
        raise ValueError(f"No job found with id '{job_id}'")

    batch_job_id = jobs[0]["jobId"]
    raw_status = jobs[0]["status"]

    # Get tags via describe_jobs
    detail = batch.describe_jobs(jobs=[batch_job_id])
    detail_jobs = detail.get("jobs", [])
    notebook = "unknown"
    if detail_jobs:
        notebook = detail_jobs[0].get("tags", {}).get("notebook", "unknown")

    return {
        "id": job_id,
        "notebook": notebook,
        "status": map_status(raw_status),
    }


def get_job_output(job_id: str) -> bytes:
    """
    Retrieves the executed notebook output from S3 for a completed job.

    The output location is derived from the job ID, matching the path written
    by the notebook container on job completion.

    Args:
        job_id (str): The unique job ID (UUID) returned by start_job().

    Returns:
        bytes: The raw contents of the executed output notebook (.ipynb).

    Raises:
        ValueError: If the output file does not exist in S3, likely because
            the job has not completed yet or failed before writing output.
        ClientError: If the AWS API call fails due to permissions or an invalid request.
    """
    s3_key = f"{job_id}{NOTEBOOK_EXTENSION}"

    try:
        response = boto3.client("s3").get_object(Bucket=S3_BUCKET, Key=s3_key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise ValueError(
                f"No output found for job '{job_id}'. "
                "The job may still be running or may have failed before writing output."
            ) from e
        raise

    return response["Body"].read()


def list_all_jobs(status_filter: str | None = None) -> list[dict]:
    """
    List jobs, optionally filtered by simplified status.

    Args:
        status_filter: One of "queued", "running", "succeeded", "failed", or None for all.

    Returns:
        List of {"id": str, "notebook": str, "status": str} dicts.

    Raises:
        ValueError: If status_filter is not a valid simplified status.
    """
    if status_filter and status_filter not in VALID_API_STATUSES:
        raise ValueError(
            f"Invalid status filter '{status_filter}'. Must be one of: {', '.join(VALID_API_STATUSES)}."
        )

    batch = boto3.client("batch")

    # Determine which AWS Batch statuses to query
    if status_filter:
        batch_statuses = API_TO_BATCH_STATUSES[status_filter]
    else:
        batch_statuses = ALL_BATCH_STATUSES

    # Collect all job summaries across the relevant statuses
    all_summaries = []
    for batch_status in batch_statuses:
        response = batch.list_jobs(
            jobQueue=JOB_QUEUE,
            jobStatus=batch_status,
        )
        for job in response.get("jobSummaryList", []):
            if job["jobName"].startswith(JOBNAME_PREFIX):
                all_summaries.append(job)

    if not all_summaries:
        return []

    # Batch call to get tags for all jobs at once
    batch_job_ids = [job["jobId"] for job in all_summaries]
    detail_response = batch.describe_jobs(jobs=batch_job_ids)

    # Build lookup: AWS job ID → notebook name
    notebook_lookup = {}
    for detail_job in detail_response.get("jobs", []):
        notebook_lookup[detail_job["jobId"]] = detail_job.get("tags", {}).get(
            "notebook", "unknown"
        )

    # Build result list
    results = []
    for job in all_summaries:
        job_uuid = extract_job_uuid(job["jobName"])
        if job_uuid:
            results.append(
                {
                    "id": job_uuid,
                    "notebook": notebook_lookup.get(job["jobId"], "unknown"),
                    "status": map_status(job["status"]),
                }
            )

    return results
