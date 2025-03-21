"""
For each metadata document in the "fits_metadata.metadata" collection:
  - Retrieve the image from the S3 bucket "corrected-ffic"
  - Compute the boundaries of the image in world coordinates
  - Query the apertures within the image boundaries
  - Compute the fluxes for each aperture and mask
  - Insert the results into the "stars.pixel_files" collection
"""

import io
import logging
import warnings
from typing import Tuple
import numpy as np
from astropy.wcs import WCS, FITSFixedWarning
import boto3
from pymongo import MongoClient

# Test configuration (TODO: move these settings to a config file)
CORRECTED_BUCKET = "corrected-ffic"
S3_ENDPOINT      = "http://localhost:9000"
ACCESS_KEY       = "minio"
SECRET_KEY       = "test123minio"
MONGO_URI = "mongodb://localhost:27017/"
META_DB = "fits_metadata"
META_COLLECTION = "metadata"
STARS_DB = "stars"
APERTURE_COLLECTION = "apertures"
PIXEL_FILES_COLLECTION = "pixel_files"


# Image dimensions with buffer (ny, nx)
IMAGE_SHAPE = (2136, 2078)

# --------------------- Logging Setup ---------------------
warnings.simplefilter('ignore', FITSFixedWarning)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def compute_fluxes(image: np.ndarray, pix_coords: np.ndarray) -> Tuple[float, float]:
    """
    Computes the sum of pixel values at the aperture positions
    And over the bounding box (expanded by 5 pixels) - minus the cluster_flux.

    Args:
        image: Image array
        pix_coords: Pixel coordinates of the aperture

    Returns:
        tuple: (cluster_flux, mask_flux)
    """
    int_coords = np.round(pix_coords).astype(int)
    xs = int_coords[:, 0]
    ys = int_coords[:, 1]
    cluster_flux = np.sum(image[ys, xs])

    # Expand bounding box by 5 pixels in each direction and clamp to image boundaries.
    x_min = max(0, np.min(xs) - 5)
    y_min = max(0, np.min(ys) - 5)
    x_max = min(IMAGE_SHAPE[1] - 1, np.max(xs) + 5)
    y_max = min(IMAGE_SHAPE[0] - 1, np.max(ys) + 5)

    bounding_box_flux = np.sum(image[y_min:y_max + 1, x_min:x_max + 1])
    mask_flux = bounding_box_flux - cluster_flux
    return cluster_flux, mask_flux


def process_metadata_document(meta_doc: dict) -> list:
    """
    Process all the apertures for a metadata document

    Args:
        meta_doc: Metadata document from the fits_metadata.metadata collection.

    Returns:
        list: List of aperture flux
    """
    filename = meta_doc["filename"]
    obs_timestamp = meta_doc["secondary_header"]["DATE-OBS"]
    logging.info("Processing %s", filename)

    # Download image data from S3
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    response = s3_client.get_object(Bucket=CORRECTED_BUCKET, Key=filename + ".npy")
    s3_client.close()
    data = response["Body"].read()
    image = np.load(io.BytesIO(data))

    # Rebuild the WCS using secondary_header
    wcs_obj = WCS(meta_doc["secondary_header"])

    # Compute the position of the image corners in world coordinates
    footprint = wcs_obj.calc_footprint().tolist()

    # Query the apertures within the image footprint
    client = MongoClient(MONGO_URI)
    aperture_coll = client[STARS_DB][APERTURE_COLLECTION]
    apertures = list(aperture_coll.find(
        {
            "centroid": {
                "$within": {"$polygon": footprint}
            }
        }
    ))
    client.close()

    # Process each aperture.
    results = []
    for aper in apertures:
        pixel_world = np.array(aper["pixels"])

        # Convert world coordinates to image pixel coordinates.
        pix_coords = wcs_obj.all_world2pix(pixel_world, 0)
        cluster_flux, mask_flux = compute_fluxes(image, pix_coords)

        result_doc = {
            "cluster_label": aper["cluster_label"],
            "obs_timestamp": obs_timestamp,
            "cluster_flux": float(cluster_flux),
            "mask_flux": float(mask_flux)
        }
        results.append(result_doc)

    return results


def main():
    """
    Process flux for each image in the corrected-ffic bucket
    """
    client = MongoClient(MONGO_URI)
    meta_db = client[META_DB]
    meta_coll = meta_db[META_COLLECTION]
    stars_db = client[STARS_DB]
    pixel_files_coll = stars_db[PIXEL_FILES_COLLECTION]

    metadata_docs = list(meta_coll.find({}))
    if not metadata_docs:
        logging.info("No metadata documents found.")
        return

    batch_results = []
    for meta_doc in metadata_docs:
        results = process_metadata_document(meta_doc)
        batch_results.extend(results)

    pixel_files_coll.insert_many(batch_results)
    logging.info("Inserted %s aperture flux documents into the %s collection.",
                 len(batch_results), PIXEL_FILES_COLLECTION)
    client.close()

if __name__ == "__main__":
    main()
