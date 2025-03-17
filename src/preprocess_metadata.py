import os
import boto3
import tempfile
from cassandra.cluster import Cluster
from astropy.io import fits
from model.AstroFileMetadata import AstroFileMetadata

# MinIO S3 client setup
s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minio",
    aws_secret_access_key="test123minio"
)

BUCKET_NAME = "raw-ffi"

# Cassandra connection
cluster = Cluster(['localhost'])  # Adjust if Cassandra is on another host
session = cluster.connect()
session.set_keyspace('fits_metadata')

def list_files():
    """List all files in the MinIO bucket."""
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    return [obj["Key"] for obj in response.get("Contents", [])]

def download_file(file_key):
    """Download a file from MinIO to a temporary location."""
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    s3_client.download_file(BUCKET_NAME, file_key, temp_file.name)
    return temp_file.name

def process_fits_file(file_path, file_key):
    """Extract metadata from a FITS file and store it in Cassandra."""
    with fits.open(file_path) as hdul:
        metadata = AstroFileMetadata.Parse_fits_file(hdul, filename=file_key)

        # Get field names & values dynamically
        metadata_dict = metadata.model_dump()

        # Check for empty fields
        for key, value in metadata_dict.items():
            if value is None or value == '':
                print(f"Warning: Field {key} is empty or None.")

        fields = ", ".join(metadata_dict.keys())  # "filename, bucket, s3_path, camera, ccd, date_obs, ..."
        placeholders = ", ".join(["%s"] * len(metadata_dict))  # "%s, %s, %s, %s, %s, ..."
        values = tuple(metadata_dict.values())

        # Dynamically construct the query
        query = f"INSERT INTO metadata ({fields}) VALUES ({placeholders})"
        session.execute(query, values)

def main():
    """Main function to process all FITS files in MinIO."""
    files = list_files()

    for file_key in files:
        file_path = download_file(file_key)
        process_fits_file(file_path, file_key)
        os.remove(file_path)  # Clean up temp file

    print("All files processed and stored in Cassandra.")

if __name__ == "__main__":
    main()
