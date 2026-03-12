"""
This module defines the `start_job` function, which submits an AWS Batch job to execute a specified notebook with given parameters. 
"""

import json
import uuid

import boto3

JOB_QUEUE = "Notebook2REST-fargate-job-queue"
JOBNAME_PREFIX = "Notebook2REST-"


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
    notebook_output_location = f"s3://notebook2rest/{job_id}.ipynb"

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

    response = boto3.client("batch").batch_client.list_jobs(
        jobQueue=JOB_QUEUE,
        filters=[{"name": "JOB_NAME", "values": [job_name]}],
    )

    jobs = response.get("jobSummaryList", [])
    if not jobs:
        raise ValueError(f"No job found with ID '{job_id}' (looked up as '{job_name}').")

    # The filter is an exact name match, so there should only ever be one result.
    return jobs[0]["status"]