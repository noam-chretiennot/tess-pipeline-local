"""
This script downloads all FITS files from a MinIO bucket,
extracts metadata (the raw HDU information) from each file, and stores the metadata in MongoDB.

For each file, it stores:
  - filename (with the ".fits" extension removed)
  - primary_header (as a dictionary)
  - secondary_header (as a dictionary, if available; otherwise None)

This method avoids using a custom parser and converts the header directly via dict().
"""

from concurrent.futures import ThreadPoolExecutor
import io
import os
from typing import Tuple, List
import boto3
from pymongo import MongoClient
from pymongo.collection import Collection
from astropy.io import fits

# Test configuration (TODO: move these settings to a config file)
RAW_BUCKET = "raw-ffic"
S3_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "test123minio"
MONGO_URI = "mongodb://localhost:27017/"

def get_s3_client() -> boto3.client:
    """
    Create and return a new S3 client to connect to the MinIO server.
    """
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

def get_mongo_collection() -> Tuple[MongoClient, Collection]:
    """
    Create and return a MongoDB client and the 'metadata' collection from the 'fits_metadata' database.
    """
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["fits_metadata"]
    return mongo_client, db["metadata"]

def list_files() -> List[str]:
    """
    List all file keys in the designated MinIO bucket.
    
    Returns:
        list: A list of file keys (strings) from the MinIO bucket.
    """
    s3_client = get_s3_client()
    try:
        response = s3_client.list_objects_v2(Bucket=RAW_BUCKET)
        return [obj["Key"] for obj in response.get("Contents", [])]
    finally:
        s3_client.close()

def process_fits_file(file_key: str) -> None:
    """
    Download a FITS file from MinIO, extract raw header information, and store the metadata in MongoDB.
    
    The metadata stored includes:
      - filename (with the ".fits" extension removed)
      - primary_header: dictionary from HDU[0].header
      - secondary_header: dictionary from HDU[1].header if available, else None
    
    Args:
        file_key (str): The key (filename) of the FITS file in the MinIO bucket.
    """
    s3_client = get_s3_client()
    mongo_client, collection = get_mongo_collection()
    try:
        response = s3_client.get_object(Bucket=RAW_BUCKET, Key=file_key)
        with fits.open(io.BytesIO(response['Body'].read())) as hdul:
            primary_header = dict(hdul[0].header)
            secondary_header = dict(hdul[1].header) if len(hdul) > 1 else None
            # Remove the ".fits" extension from filename
            stored_filename = os.path.splitext(file_key)[0]
            metadata = {
                "filename": stored_filename,
                "primary_header": primary_header,
                "secondary_header": secondary_header
            }
            collection.insert_one(metadata)
    finally:
        s3_client.close()
        mongo_client.close()

def main():
    """
    Process all FITS files from MinIO and store their raw HDU metadata in MongoDB in parallel.
    """
    files = list_files()
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_fits_file, file_key) for file_key in files]
        for future in futures:
            future.result()
    print("All files processed and stored in MongoDB.")

if __name__ == "__main__":
    main()
