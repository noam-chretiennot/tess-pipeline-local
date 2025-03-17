"""
Module to initialize Minio with necessary S3 buckets.
"""

import argparse
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from cassandra.cluster import Cluster
from model.AstroFileMetadata import AstroFileMetadata


def init_localstack(endpoint, access_key, secret_key):
    """Create necessary S3 buckets."""
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


def create_table():
    """Create a table in Cassandra to store FITS file metadata."""
    cluster = Cluster(['localhost'])
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS fits_metadata
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)

    session.set_keyspace('fits_metadata')

    columns = []
    for field, field_type in AstroFileMetadata.__annotations__.items():
        cassandra_type = {
            int: "INT",
            float: "DOUBLE",
            str: "TEXT",
            datetime: "TIMESTAMP"
        }.get(field_type, "TEXT")  # Default to TEXT if unknown

        columns.append(f"{field} {cassandra_type}")
    columns[0] += " PRIMARY KEY"

    table_schema = ",\n    ".join(columns)
    create_table_query = f"CREATE TABLE IF NOT EXISTS metadata (\n    {table_schema}\n);"
    session.execute(create_table_query)

    print("Cassandra table created successfully.")


def main():
    """Parse arguments and initialize S3."""
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

    create_table()


if __name__ == "__main__":
    main()
