import boto3

ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
REGION = "eu-west-1"
