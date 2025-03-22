from fastapi import APIRouter, HTTPException
import random
import io
from astropy.time import Time
from app.config import mongo_client

router = APIRouter()

@router.get("/curated")
def get_cluster_data():
    """
    Randomly selects a cluster label and returns both light curve and aperture data for that cluster.
    Light curve data is gathered from stars.pixel_files.
    Aperture data is gathered from stars.apertures.
    """
    try:
        stars_db = mongo_client["stars"]
        pixel_coll = stars_db["pixel_files"]
        
        # Get distinct cluster labels
        distinct_labels = pixel_coll.distinct("cluster_label")
        if not distinct_labels:
            raise HTTPException(status_code=404, detail="No cluster labels found in pixel_files collection.")
        
        chosen_label = random.choice(distinct_labels)
        
        # Retrieve light curve data from pixel_files
        docs = list(pixel_coll.find({"cluster_label": chosen_label}).sort("obs_timestamp", 1))
        if not docs:
            raise HTTPException(status_code=404, detail=f"No records found for cluster label: {chosen_label}")
        
        timestamps = []
        cluster_fluxes = []
        mask_fluxes = []
        for d in docs:
            ts = d.get("obs_timestamp")
            dt = None
            if isinstance(ts, (int, float)):
                try:
                    dt = Time(ts, format="mjd").datetime
                except Exception as e:
                    pass
            elif isinstance(ts, str):
                try:
                    dt = Time(ts).datetime
                except Exception as e:
                    pass
            if dt is not None:
                # Return ISO format for easier JSON handling
                timestamps.append(dt.isoformat())
                cluster_fluxes.append(d.get("cluster_flux", 0))
                mask_fluxes.append(d.get("mask_flux", 0))
        
        # Retrieve aperture data from apertures collection
        apertures_coll = stars_db["apertures"]
        cluster_aperture = apertures_coll.find_one({"cluster_label": chosen_label})
        if cluster_aperture:
            pixels = cluster_aperture.get("pixels", [])
            centroid = cluster_aperture.get("centroid", None)
        else:
            pixels = []
            centroid = None
        
        return {
            "cluster_label": chosen_label,
            "light_curve": {
                "timestamps": timestamps,
                "cluster_fluxes": cluster_fluxes,
                "mask_fluxes": mask_fluxes
            },
            "aperture": {
                "pixels": pixels,
                "centroid": centroid
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
