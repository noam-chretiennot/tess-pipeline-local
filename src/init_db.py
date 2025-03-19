"""
Module to initialize Minio and MongoDB before unpacking the files.
"""

import argparse
import boto3
from botocore.exceptions import ClientError
from pymongo import MongoClient


def init_localstack(endpoint, access_key, secret_key):
    """Create necessary S3 buckets."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    print("Creating buckets in S3...")

    buckets = ["raw-ffic", "corrected-ffic"]

    for bucket in buckets:
        try:
            s3_client.create_bucket(Bucket=bucket)
            print(f"Bucket {bucket} created successfully.")
        except ClientError as e:
            print(f"Error creating bucket {bucket}: {e}")

    list_buckets = s3_client.list_buckets()
    print(f"List of buckets: {[b['Name'] for b in list_buckets.get('Buckets', [])]}")


def create_collection():
    """
    Initialize MongoDB collection to store FITS file metadata.
    This replaces the Cassandra table creation.
    """
    client = MongoClient("mongodb://localhost:27017/")
    db = client["fits_metadata"]
    collection = db["metadata"]
    print("MongoDB collection initialized successfully.")
    return collection


def main():
    """Parse arguments and initialize S3 and MongoDB."""
    parser = argparse.ArgumentParser(description="Initialize LocalStack with S3 buckets")
    parser.add_argument("--endpoint", type=str,
                        default="http://localhost:9000",
                        help="LocalStack S3 endpoint")
    parser.add_argument("--access_key", type=str,
                        default="minio",
                        help="LocalStack access key")
    parser.add_argument("--secret_key", type=str,
                        default="test123minio",
                        help="LocalStack secret key")

    args = parser.parse_args()
    init_localstack(args.endpoint, args.access_key, args.secret_key)

    # Initialize MongoDB collection (replaces the Cassandra table creation)
    create_collection()


if __name__ == "__main__":
    main()
