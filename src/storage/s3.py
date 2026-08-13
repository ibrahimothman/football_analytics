import boto3
import os

from src.config.settings import (
    S3_ENDPOINT_URL,
    S3_REGION,
)


def get_s3_client():

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name=S3_REGION,
    )