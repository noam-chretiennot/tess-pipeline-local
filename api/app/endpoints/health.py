from fastapi import APIRouter
from app.config import s3_client, mongo_client

router = APIRouter()

@router.get("/health")
def health():
    status = {"api": "OK"}
    try:
        s3_client.list_buckets()
        status["minio"] = "OK"
    except Exception as e:
        status["minio"] = f"Error: {str(e)}"
    try:
        mongo_client.admin.command('ping')
        status["mongodb"] = "OK"
    except Exception as e:
        status["mongodb"] = f"Error: {str(e)}"
    return status
