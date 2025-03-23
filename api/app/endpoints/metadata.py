from fastapi import APIRouter, HTTPException
from app.config import meta_coll

router = APIRouter()

@router.get("/metadata/values")
def metadata_values():
    try:
        cameras = meta_coll.distinct("secondary_header.CAMERA")
        ccds = meta_coll.distinct("secondary_header.CCD")
        date_obs = meta_coll.distinct("secondary_header.DATE-OBS")
        return {"CAMERA": cameras, "CCD": ccds, "DATE-OBS": date_obs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
