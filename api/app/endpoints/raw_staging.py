from fastapi import APIRouter, HTTPException, Query
from app.config import s3_client, RAW_BUCKET, STAGING_BUCKET

router = APIRouter()

@router.get("/raw")
def get_raw(
    obs_date: str = Query(None),
    camera: str = Query(None),
    ccd: str = Query(None)
):
    try:
        resp = s3_client.list_objects_v2(Bucket=RAW_BUCKET)
        objects = resp.get("Contents", [])
        filtered = []
        for obj in objects:
            key = obj["Key"]
            match = True
            if obs_date and obs_date not in key:
                match = False
            if camera and camera not in key:
                match = False
            if ccd and ccd not in key:
                match = False
            if match:
                filtered.append({"Key": key, "Size": obj["Size"]})
        return {"bucket": RAW_BUCKET, "objects": filtered}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/staging")
def get_staging(
    obs_date: str = Query(None),
    camera: str = Query(None),
    ccd: str = Query(None)
):
    try:
        resp = s3_client.list_objects_v2(Bucket=STAGING_BUCKET)
        objects = resp.get("Contents", [])
        filtered = []
        for obj in objects:
            key = obj["Key"]
            match = True
            if obs_date and obs_date not in key:
                match = False
            if camera and camera not in key:
                match = False
            if ccd and ccd not in key:
                match = False
            if match:
                filtered.append({"Key": key, "Size": obj["Size"]})
        return {"bucket": STAGING_BUCKET, "objects": filtered}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
