from fastapi import APIRouter
from app.config import s3_client, RAW_BUCKET, STAGING_BUCKET, mongo_client

router = APIRouter()

@router.get("/stats")
def stats():
    # Bucket metrics
    bucket_metrics = {}
    for bucket in [RAW_BUCKET, STAGING_BUCKET]:
        try:
            resp = s3_client.list_objects_v2(Bucket=bucket)
            objects = resp.get("Contents", [])
            count = len(objects)
            total_size = sum(obj["Size"] for obj in objects)
        except Exception as e:
            count = 0
            total_size = 0
        bucket_metrics[bucket] = {"object_count": count, "total_size": total_size}
    
    # Collection metrics: loop over all non-system databases and their collections
    collections_metrics = {}
    system_dbs = ["admin", "config", "local"]
    for db_name in mongo_client.list_database_names():
        if db_name in system_dbs:
            continue
        db_obj = mongo_client[db_name]
        coll_names = db_obj.list_collection_names()
        collections_metrics[db_name] = {}
        for coll_name in coll_names:
            try:
                count = db_obj[coll_name].count_documents({})
            except Exception as e:
                count = f"Error: {e}"
            collections_metrics[db_name][coll_name] = count
    return {"buckets": bucket_metrics, "collections": collections_metrics}
