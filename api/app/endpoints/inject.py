from fastapi import APIRouter, UploadFile, File, HTTPException
import uuid
import time
import io
from astropy.io import fits
from app.config import s3_client, RAW_BUCKET, meta_coll

router = APIRouter()

@router.post("/inject/")
async def inject(file: UploadFile = File(...)):
    file_id = str(uuid.uuid4())
    file_key = f"{file_id}_{file.filename}"
    try:
        # Upload the file to the raw bucket
        s3_client.upload_fileobj(
            file.file,
            RAW_BUCKET,
            file_key,
            ExtraArgs={"ContentType": file.content_type},
        )
        # Retrieve file content from MinIO for FITS metadata extraction
        response = s3_client.get_object(Bucket=RAW_BUCKET, Key=file_key)
        content = response['Body'].read()
        with fits.open(io.BytesIO(content)) as hdul:
            primary_header = dict(hdul[0].header)
            secondary_header = dict(hdul[1].header) if len(hdul) > 1 else None
            stored_filename = file_key.rsplit(".", 1)[0]
            metadata = {
                "upload_time": time.time(),
                "filename": stored_filename,
                "primary_header": primary_header,
                "secondary_header": secondary_header
            }
        # Insert metadata into MongoDB
        result = meta_coll.insert_one(metadata)
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to insert metadata into MongoDB")
        return {"message": "FITS uploaded and metadata indexed", "filename": stored_filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
