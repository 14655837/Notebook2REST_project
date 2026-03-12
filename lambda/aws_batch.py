"""
This module provides functions for interacting with AWS Batch and S3 to execute Jupyter notebooks
as batch jobs. It exposes three functions:

- `start_job`: Submits an AWS Batch job to execute a specified notebook with given parameters.
- `get_job_status`: Retrieves the current status of a submitted batch job.
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
    )

    return job_id


def get_job_status(job_id: str) -> str:
    """
    Retrieves the status of a submitted AWS Batch job using its unique job ID.

    Uses a JOB_NAME filter for a direct lookup, avoiding a full queue scan.

    Args:
        job_id (str): The unique job ID (UUID) assigned when the job was submitted via start_job().

    Returns:
        str: The current status of the job: "SUBMITTED", "PENDING", "RUNNABLE",
            "STARTING", "RUNNING", "SUCCEEDED", or "FAILED".

    Raises:
        Error: If no job is found with the given job ID.
    """

    job_name = f"{JOBNAME_PREFIX}{job_id}"

    response = boto3.client("batch").list_jobs(
        jobQueue=JOB_QUEUE,
        filters=[{"name": "JOB_NAME", "values": [job_name]}],
    )

    jobs = response.get("jobSummaryList", [])
    if not jobs:
        raise ValueError(
            f"No job found with ID '{job_id}' (looked up as '{job_name}')."
        )

    # The filter is an exact name match, so there should only ever be one result.
    return jobs[0]["status"]


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
