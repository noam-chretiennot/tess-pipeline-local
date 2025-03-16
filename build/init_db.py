import os
import time
import argparse
import boto3
from botocore.exceptions import ClientError


def init_localstack(endpoint, access_key, secret_key):
    """Create necessary S3 buckets in LocalStack."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    print("Creating buckets in S3...")

    buckets = ["raw-ffi", "silver-light-curves", "cache-ffi-cuts"]

    for bucket in buckets:
        try:
            s3_client.create_bucket(Bucket=bucket)
            print(f"Bucket {bucket} created successfully.")
        except ClientError as e:
            print(f"Error creating bucket {bucket}: {e}")

    list_buckets = s3_client.list_buckets()
    print(f"List of buckets: {[b['Name'] for b in list_buckets.get('Buckets', [])]}")


def main():
    """Parse arguments and initialize LocalStack S3."""
    parser = argparse.ArgumentParser(description="Initialize LocalStack with S3 buckets")
    parser.add_argument("--endpoint", type=str, required=True, help="LocalStack S3 endpoint")
    parser.add_argum
