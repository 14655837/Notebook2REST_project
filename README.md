# Notebook2REST

This project is about turning Jupyter notebooks into something we can call through a REST API. Instead of running a notebook by hand, the idea is that we can send a request, execute it in the cloud, and get the result back in a more structured way.

We built this around FastAPI, Docker, Papermill, and AWS. A request comes in through the API, the notebook is started as an AWS Batch job, and the executed notebook is stored in S3.

We made this in collaboration with a client from LifeWatch, based on the need to make notebook-based workflows easier to deploy and reuse without rebuilding everything by hand after every change.

## What this project does

The main goal is to make notebooks easier to use as services instead of standalone files. In our setup, that means:

- extracting notebook parameters automatically,
- exposing notebooks through an API,
- running them inside containers,
- and storing the output after execution.

The idea is simple: push a notebook, then have a matching REST-accessible service instead of a manual notebook workflow.

## How it works

The project has a few main parts:

### Parameter extraction

`api_repo/pipeline.py` looks through notebooks and collects variables that start with `param_`. Those are written into `paramdump.json`, which is then used by the API to know which notebooks exist and which parameters they accept.

### API layer

`app.py` contains the FastAPI app. This is the part that handles requests such as:

- starting a notebook run,
- checking the status of a job,
- listing running jobs,
- and getting the S3 location of the output notebook.

### Notebook execution

`docker/run_from_json.py` handles the actual notebook run inside the container. It reads the given parameters, patches the notebook, and executes it with Papermill.

The Docker image is defined in `docker/dockerfile`.

### AWS integration

`lambda/aws_batch.py` connects the API to AWS Batch and S3. It submits jobs, checks their status, and helps find the output notebook after execution.

## Request flow

This is the basic flow:

1. A client sends a `POST` request for a notebook.
2. The API checks `paramdump.json` for the notebook and its default parameters.
3. If extra parameters are included, they are validated first.
4. The API submits an AWS Batch job.
5. The notebook runs inside a container with Papermill.
6. The output notebook is written to S3.
7. The client can later check the status or retrieve the output location.

## API routes

Right now the API has these routes:

- `GET /`
- `GET /notebook/list`
- `GET /notebook/{job_id}`
- `GET /latest-notebook`
- `GET /notebook/{job_id}/uri`
- `POST /notebook/{notebook_name}`

Example body for starting a notebook:

```json
{
  "param_example": 42
}
```

## Repository structure

- `app.py` - FastAPI app
- `lambda/aws_batch.py` - AWS Batch and S3 helper functions
- `lambda/config.py` - AWS config
- `api_repo/pipeline.py` - notebook parameter extraction
- `api_repo/pipeline.sh` - older helper script for notebook conversion
- `docker/run_from_json.py` - notebook execution script
- `docker/dockerfile` - Docker image definition
- `nb2rest.sh` - helper script to invoke the Lambda function

## Important assumptions

There are a few things the current code expects:

- the AWS resources already exist,
- the S3 bucket is called `notebook2rest`,
- the AWS region is `eu-west-1`,
- `paramdump.json` has been generated before the API is used,
- and notebook parameters use the `param_` prefix.

## Dependencies

Some of the main dependencies used in this repo are:

- FastAPI
- Uvicorn
- Papermill
- nbformat / nbconvert / Jupyter
- GitPython
- boto3

## Helper script

`nb2rest.sh` can be used to call the deployed Lambda function with a simulated API Gateway event.

Example:

```bash
./nb2rest.sh POST /notebook/LiDAR_Vlab_tutorial.ipynb
```

## Team

- Jona Aalten
- Mike van der Deure
- Theo Gatea
- Colin de Koning
