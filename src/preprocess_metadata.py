"""
This script downloads all FITS files from a MinIO bucket,
extracts metadata from them, and stores the metadata in a Cassandra database.
"""
import tempfile
import boto3
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

def process_fits_file(file_path, file_key):
    """Extract metadata from a FITS file and store it in Cassandra."""
    with fits.open(file_path) as hdul:
        metadata = AstroFileMetadata.parse_fits_file(hdul, filename=file_key)

        # Get field names & values dynamically
        metadata_dict = metadata.model_dump()

        # Check for empty fields
        for key, value in metadata_dict.items():
            if value is None or value == '':
                print(f"Warning: Field {key} is empty or None.")

        fields = ", ".join(metadata_dict.keys())
        placeholders = ", ".join(["%s"] * len(metadata_dict))
        values = tuple(metadata_dict.values())

        # Dynamically construct the query
        query = f"INSERT INTO metadata ({fields}) VALUES ({placeholders})"
        session.execute(query, values)

def main():
    """Main function to process all FITS files in MinIO."""
    files = list_files()

    for file_key in files:
        with tempfile.NamedTemporaryFile() as temp_file:
            # Download the file to the temporary location
            s3_client.download_file(BUCKET_NAME, file_key, temp_file.name)
            
            # Process the FITS file
            process_fits_file(temp_file.name, file_key)

    print("All files processed and stored in Cassandra.")

if __name__ == "__main__":
    main()
