"""
This module defines the `start_job` function, which submits an AWS Batch job to execute a specified notebook with given parameters. 
"""

import json
import uuid

import boto3

from config import ACCOUNT_ID, REGION

ECR_REPO = "notebook2rest"
JOB_QUEUE = "Notebook2REST-fargate-job-queue"
JOB_DEFINITION = "Notebook2REST-fargate-job-definition"


def start_job(notebook: str, params: dict) -> str:
    """
    Submits an AWS Batch job to execute the specified notebook with the given parameters.

    Args:
        notebook (str): The name of the notebook (used to resolve the ECR image). The name should exactly match the image name in ECR (without the tag).
        params (dict): A dictionary of parameters to pass to the notebook container. Passed as a single json string environment variable to the container.

    Returns:
        str: A unique job ID (UUID) identifying the submitted batch job.
    """

    job_id = str(uuid.uuid4())  # Create a unique job ID
    image_uri = (
        f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{ECR_REPO}/{notebook}:latest"
    )

    boto3.client("batch").submit_job(
        jobName=f"notebook-{notebook}",
        jobQueue=JOB_QUEUE,
        jobDefinition=JOB_DEFINITION,
        containerOverrides={
            "image": image_uri,
            "environment": [
                {"name": "JOB_ID", "value": job_id},
                {"name": "NOTEBOOK_PARAMS", "value": json.dumps(params)},
            ],
        },
    )

    return job_id
