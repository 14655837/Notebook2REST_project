"""
This module defines the `start_job` function, which submits an AWS Batch job to execute a specified notebook with given parameters. 
"""

import json
import uuid

import boto3

JOB_QUEUE = "Notebook2REST-fargate-job-queue"


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
        jobName=f"Notebook2REST-{job_id}",
        jobQueue=JOB_QUEUE,
        jobDefinition=f"Notebook2REST-{notebook}",
        containerOverrides={
            "environment": [
                {"name": "NOTEBOOK_OUT", "value": notebook_output_location},
                {"name": "NOTEBOOK_PARAMS", "value": json.dumps(params)},
            ],
        },
    )

    return job_id