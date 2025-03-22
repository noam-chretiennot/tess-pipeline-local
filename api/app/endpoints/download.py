from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from app.config import s3_client

router = APIRouter()

@router.get("/download")
def download_file(bucket: str, key: str):
    """
    Returns the file content from the specified bucket and key using streaming.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content_type = response.get("ContentType", "application/octet-stream")
        return StreamingResponse(response["Body"], media_type=content_type)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
