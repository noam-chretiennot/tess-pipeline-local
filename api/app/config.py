import boto3
from pymongo import MongoClient

S3_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "test123minio"
RAW_BUCKET = "raw-ffic"
STAGING_BUCKET = "corrected-ffic"
MONGO_URI = "mongodb://mongodb:27017/"


s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)
mongo_client = MongoClient(MONGO_URI)

fits_metadata_db = mongo_client["fits_metadata"]
meta_coll = fits_metadata_db["metadata"]
