import boto3
import requests
from botocore.exceptions import ClientError
import dotenv
import time
import os
import argparse

def init_localstack(endpoint, access_key, secret_key):
    """Create necessary buckets in S3"""
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
    print(f"List of buckets: {list_buckets}")


def main():
    parser = argparse.ArgumentParser(description="Initialize localstack")
    parser.add_argument("--endpoint", type=str, required=True, help="Localstack endpoint")
    parser.add_argument("--access_key", type=str, required=True, help="Localstack access key")
    parser.add_argument("--secret_key", type=str, required=True, help="Localstack secret key")
    args = parser.parse_args()

    init_localstack(args.endpoint, args.access_key, args.secret_key)

if __name__ == "__main__":
    init_localstack("http://localhost:9000", "minio", "test123minio")

