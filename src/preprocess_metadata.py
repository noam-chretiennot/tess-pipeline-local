"""
This script downloads all FITS files from a MinIO bucket,
extracts metadata from each file, and stores the metadata in MongoDB.
"""

from concurrent.futures import ThreadPoolExecutor
import io
from typing import Tuple, List
import boto3
from pymongo import MongoClient
from pymongo.collection import Collection
from astropy.io import fits
from model.AstroFileMetadata import AstroFileMetadata

# Test configuration (TODO: move these settings to a config file)
RAW_BUCKET = "raw-ffic"
CORRECTED_BUCKET = "corrected-ffic"
S3_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "test123minio"
MONGO_URI = "mongodb://localhost:27017/"

def get_s3_client() -> boto3.client:
    """
    Create and return a new S3 client to connect to the MinIO server.

    Returns:
        boto3.client: A new boto3 S3 client configured for the MinIO endpoint.
    """
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

def get_mongo_collection() -> Tuple[MongoClient, Collection]:
    """
    Create and return a MongoDB client and the collection for storing FITS metadata.

    Returns:
        tuple: A tuple (mongo_client, collection) where mongo_client is a MongoClient instance,
               and collection is the 'metadata' collection from the 'fits_metadata' database.
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
        # Extract and return the file keys from the response, if any.
        return [obj["Key"] for obj in response.get("Contents", [])]
    finally:
        s3_client.close()  # Ensure the S3 client connection is properly closed.

def process_fits_file(file_key: str) -> None:
    """
    Download a FITS file from MinIO, extract metadata from it, and store the metadata in MongoDB.

    This function streams the FITS file associated with the given file key from the MinIO bucket,
    extracts metadata using the AstroFileMetadata class, and then inserts the metadata into 
    the MongoDB 'metadata' collection.

    Args:
        file_key (str): The key (filename) of the FITS file in the MinIO bucket.
    """
    s3_client = get_s3_client()
    mongo_client, collection = get_mongo_collection()
    try:
        # Retrieve the FITS file from the S3 bucket.
        response = s3_client.get_object(Bucket=RAW_BUCKET, Key=file_key)

        with fits.open(io.BytesIO(response['Body'].read())) as hdul:
            # Extract metadata from the FITS file using AstroFileMetadata.
            metadata = AstroFileMetadata\
                .parse_fits_file(hdul, filename=file_key)\
                .model_dump()

            collection.insert_one(metadata)
    finally:
        # Close the S3 and MongoDB connections
        s3_client.close()
        mongo_client.close()

def main():
    """
    Process all FITS files from MinIO and store their metadata in MongoDB. In parrallel.
    """
    files = list_files()

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_fits_file, file_key) for file_key in files]
        for future in futures:
            future.result()
    print("All files processed and stored in MongoDB.")

if __name__ == "__main__":
    main()
