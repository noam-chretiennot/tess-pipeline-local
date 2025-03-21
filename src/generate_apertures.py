"""
Get apertures for astrosismic signal analysis

For each vue:
    - Download a processed image from S3
    - Cluster high-intensity pixels using DBSCAN
    - Refine clusters using watershed segmentation
    - Convert cluster coordinates to world coordinates
    - Store each detected cluster (aperture) as a document in MongoDB

Documentation:
  https://iopscience.iop.org/article/10.3847/1538-3881/ac09f1/pdf
"""

import io
import logging
import warnings
from typing import Tuple
import numpy as np
from astropy.wcs import WCS, FITSFixedWarning
from sklearn.cluster import DBSCAN
from scipy.ndimage import distance_transform_edt, maximum_filter, label as ndi_label
from skimage.segmentation import watershed
from dask import delayed, compute
from dask.diagnostics import ProgressBar
from pymongo import MongoClient
import boto3


# Test configuration (TODO: move these settings to a config file)
CORRECTED_BUCKET = "corrected-ffic"
S3_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "test123minio"
MONGO_URI = "mongodb://localhost:27017/"


# --------------------- Logging Configuration -------------------------
warnings.simplefilter('ignore', FITSFixedWarning)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --------------------- Clustering & Segmentation ---------------------
def flux_threshold(image: np.ndarray) -> float:
    """
    Compute the flux threshold (see documentation)

    Formula: mode + 0.8 * MAD
    """
    flat = image.flatten()
    hist, bin_edges = np.histogram(flat, bins=100)
    mode_val = (bin_edges[np.argmax(hist)] + bin_edges[np.argmax(hist) + 1]) / 2.0
    median_val = np.median(flat)
    mad = np.median(np.abs(flat - median_val))
    return mode_val + 0.8 * mad


def filtered_dbscan(image: np.ndarray, eps: float = 1.5, min_samples: int = 4) -> Tuple[np.ndarray, np.ndarray]:
    """
    Cluster high-intensity pixels using DBSCAN.

    Args:
        image: Image data
        eps: Maximum distance between pixels considered as neighborhood
        min_samples: Minimum number of samples required for a core point

    Returns:
        tuple: (high_intensity_pixels, labels)
            - high_intensity_pixels: Coordinates of pixels above threshold
            - labels: Cluster labels for the high intensity pixels
    """
    threshold = flux_threshold(image)
    high_intensity_pixels = np.argwhere(image > threshold)

    if high_intensity_pixels.size == 0:
        raise ValueError("No high-intensity pixels found.")

    eps = 1.5 # max neighborhood distance
    min_samples = 4 # filter clusters with fewer than 4 pixels
    db = DBSCAN(eps=eps, min_samples=min_samples)

    labels = db.fit_predict(high_intensity_pixels)

    return high_intensity_pixels, labels


def watershed_patch(args) -> Tuple[int, int, int, int, np.ndarray]:
    """
    Process a patch of a cluster using watershed segmentation.
    Only returns the patch if it meets the minimum pixel requirement.

    Args:
        row_min (int): Minimum row index of the patch
        row_max (int): Maximum row index of the patch
        col_min (int): Minimum column index of the patch
        col_max (int): Maximum column index of the patch
        patch (np.ndarray) : Patch of the image
        min_pixels (int): Minimum number of pixels required for watershed segmentation

    Returns:
        tuple: (row_min, row_max, col_min, col_max, ws_result)
            - ws_result: Watershed result for the patch
    """
    row_min, row_max, col_min, col_max, patch, min_pixels = args

    # Check if the patch contains enough foreground pixels
    if patch.sum() < min_pixels:
        ws_result = patch.astype(int)
    else:
        # Compute the distance from each foreground pixel to the nearest background pixel
        dist = distance_transform_edt(patch)
        # Identify local peaks in a 2x2 neighborhood
        local_max = dist == maximum_filter(dist, size=2)
        # Keep only peaks inside the patch and label connected groups as markers
        markers, _ = ndi_label(local_max * patch)

        # If there's only one marker, no need to divide the cluster
        if markers.max() < 2:
            ws_result = patch.astype(int)
        else:
            # Apply watershed segmentation
            ws_result = watershed(-dist, markers, mask=patch)
            ws_result = ws_result.astype(int)

    return (row_min, row_max, col_min, col_max, ws_result)


def get_apertures(meta_doc: dict) -> dict:
    """
    Execute the star processing pipeline:
      - Download the processed image (.npy) from S3.
      - Reconstruct the FITS file using stored headers.
      - Generate a WCS object from the FITS header.
      - Cluster high-intensity pixels using DBSCAN and refine with watershed segmentation.
      - Convert pixel coordinates to world coordinates.

    Args:
        meta_doc (dict): Metadata document from MongoDB containing stored FITS headers.

    Returns:
        list: A list of dictionaries mapping cluster labels to centroid and pixel world coordinates.

    Raises:
        ValueError: If metadata document or required headers are missing.
    """
    filename = meta_doc["filename"]

    # Download the image from S3
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    with io.BytesIO() as mem_file:
        s3_client.download_fileobj(CORRECTED_BUCKET, filename + ".npy", mem_file)
        mem_file.seek(0)
        image = np.load(mem_file)
    s3_client.close()

    # Create a WCS object from the secondary header
    secondary_header_dict = meta_doc["secondary_header"]
    wcs_obj = WCS(secondary_header_dict)

    # Cluster high-intensity pixels
    hi_pixels, labels = filtered_dbscan(image)
    seg = np.full(image.shape, -1, dtype=int)
    seg[hi_pixels[:, 0], hi_pixels[:, 1]] = labels

    # Prepare watershed tasks
    min_pixels_for_watershed = 8
    mask = seg != -1
    all_rows, all_cols = np.nonzero(mask)
    all_labels = seg[mask]

    # Sort these pixels by their label so we can group them efficiently
    order = np.argsort(all_labels)
    sorted_labels = all_labels[order]
    sorted_rows = all_rows[order]
    sorted_cols = all_cols[order]

    # Find unique labels along with the first index and count for each group
    unique_labels, first_indices, counts = np.unique(sorted_labels,
                                                     return_index=True,
                                                     return_counts=True)

    tasks = []
    for i, l in enumerate(unique_labels):
        # Determine the smallest patch necessary to contain the cluster
        start = first_indices[i]
        end = start + counts[i]
        row_min = sorted_rows[start:end].min()
        row_max = sorted_rows[start:end].max() + 1
        col_min = sorted_cols[start:end].min()
        col_max = sorted_cols[start:end].max() + 1
        patch = (seg[row_min:row_max, col_min:col_max] == l)

        task_data = (row_min, row_max, col_min, col_max, patch, min_pixels_for_watershed)
        tasks.append(delayed(watershed_patch)(task_data))

    # Execute watershed tasks in parallel.
    with ProgressBar():
        results = compute(*tasks, scheduler='processes')

    # Update segmentation with new labels from watershed splits using vectorized mapping
    new_label = seg.max() + 1
    for res in results:
        row_min, row_max, col_min, col_max, ws_result = res

        # Identify unique watershed sub-labels (ignoring zeros)
        unique_subs = np.unique(ws_result)
        unique_subs = unique_subs[unique_subs > 0]

        # Create a new block of consecutive labels for these subregions
        new_labels = np.arange(new_label, new_label + unique_subs.size)
        new_label += unique_subs.size

        # Build a mapping for all pixels in the watershed result
        mask = ws_result > 0
        # np.searchsorted works since unique_subs is sorted
        mapped = np.zeros_like(ws_result, dtype=int)
        mapped[mask] = new_labels[np.searchsorted(unique_subs, ws_result[mask])]

        # Update the segmentation within the current patch
        region = seg[row_min:row_max, col_min:col_max].copy()
        region[mask] = mapped[mask]
        seg[row_min:row_max, col_min:col_max] = region

    # Get refined labels at the high-intensity pixel positions
    refined_labels = seg[hi_pixels[:, 0], hi_pixels[:, 1]]

    # Convert clusters from pixel to world coordinates.
    clusters = []
    unique_labels = np.unique(refined_labels)
    unique_labels = unique_labels[unique_labels != -1]
    for lab in unique_labels:
        # Select the pixels in the current cluster
        cluster_mask = refined_labels == lab
        pixel_indices = hi_pixels[cluster_mask]

        # Skip clusters with fewer than 4 pixels (considered noise)
        if pixel_indices.shape[0] < 4:
            continue

        centroid_pixel = pixel_indices.mean(axis=0)

        # Convert the centroid (note that wcs expects (col, row) order)
        centroid_world = wcs_obj.all_pix2world(centroid_pixel[1], centroid_pixel[0], 0)
        centroid_world = tuple(np.asarray(centroid_world).flatten())

        # Convert all pixel coordinates to world coordinates
        world_coords = wcs_obj.all_pix2world(pixel_indices[:, 1], pixel_indices[:, 0], 0)
        world_coords = np.column_stack(world_coords).tolist()

        clusters.append({
            "cluster_label": f"{filename}_{lab}",
            "centroid": centroid_world,
            "pixels": world_coords
        })

    return clusters


# --------------------- Main Pipeline ---------------------
def main():
    """
    Execute the complete star processing pipeline:
      - Retrieve metadata from MongoDB.
      - For each unique (camera, CCD) pair (oldest file), process the image.
      - Store each detected cluster (aperture) as a document in MongoDB.
    """
    client = MongoClient(MONGO_URI)

    db = client["fits_metadata"]
    collection = db["metadata"]
    # Get the oldest file for each unique (camera, CCD) pair #TODO: use a WCS-based rule
    query = [
        {"$sort": {
            "secondary_header.CAMERA": 1,
            "secondary_header.CCD": 1,
            "secondary_header.date_obs": 1
        }},
        {"$group": {
            "_id": {
                "camera": "$secondary_header.CAMERA",
                "ccd": "$secondary_header.CCD"
            },
            "doc": {"$first": "$$ROOT"}
        }}
    ]
    result = list(collection.aggregate(query))
    files_to_process = [entry["doc"] for entry in result]


    db = client["stars"]
    collection = db["apertures"]
    for doc in files_to_process:
        clusters_dict = get_apertures(doc)
        res = collection.insert_many(clusters_dict)
        logging.info("Inserted %s documents for %s.", len(res.inserted_ids), doc["filename"])

    client.close()

if __name__ == "__main__":
    main()
