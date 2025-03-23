from fastapi import FastAPI
from app.endpoints import inject, download, health, stats, raw_staging, metadata, curated

app = FastAPI(title="Uploader & FITS Metadata Extractor")

app.include_router(inject.router)
app.include_router(download.router)
app.include_router(health.router)
app.include_router(stats.router)
app.include_router(raw_staging.router)
app.include_router(metadata.router)
app.include_router(curated.router)
