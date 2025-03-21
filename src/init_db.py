"""
Module to initialize Minio and MongoDB before unpacking the files.
"""

import boto3
from botocore.exceptions import ClientError
from pymongo import MongoClient

# Test configuration (TODO: move these settings to a config file)
S3_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "test123minio"
MONGO_URI = "mongodb://localhost:27017/"


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


def create_collection(mongo_uri):
    """
    Initialize MongoDB collection to store FITS file metadata.
    """
    client = MongoClient(mongo_uri)

    # Initialize fits_metadata collections
    meta_db = client["fits_metadata"]
    meta_coll = meta_db["metadata"]

    # Initialize stars collection
    stars_db = client["stars"]
    aperture_coll = stars_db["apertures"]
    pixel_files_coll = stars_db["pixel_files"]

    aperture_coll.create_index([("centroid", "2d")], min=-360, max=360)
    aperture_coll.create_index([("cluster_label", 1)])

    print("MongoDB collections and indexes initialized successfully.")
    return aperture_coll


def main():
    """Parse arguments and initialize S3 and MongoDB."""
    init_localstack(S3_ENDPOINT, ACCESS_KEY, SECRET_KEY)

    create_collection(MONGO_URI)


if __name__ == "__main__":
    main()
