"""
This script removes the sky background from calibrated TESS FFI images.
It uses the exact same functions as the test pipeline, but adapts them to:
  - Retrieve FFI metadata from MongoDB,
  - Download the raw calibrated FITS file from the MinIO S3 bucket "raw-ffic",
  - Process the image :
    - crop
    - subtract background
    - remove corner glow
  - Upload the processed image to the "corrected-ffic" bucket,

Documentation:
  https://archive.stsci.edu/missions/tess/doc/TESS_Instrument_Handbook_v0.1.pdf#page24
  https://iopscience.iop.org/article/10.3847/1538-3881/ac09f1/pdf
"""

import gc
from functools import reduce
from concurrent.futures import ProcessPoolExecutor, as_completed
import io
from typing import Tuple, List, Dict
import argparse
from datetime import datetime
import numpy as np
import boto3
from botocore.exceptions import ClientError
from astropy.io import fits
from pymongo import MongoClient
from scipy.ndimage import median_filter
from scipy.interpolate import RectBivariateSpline


# Test configuration (TODO: move these settings to a config file)
RAW_BUCKET = "raw-ffic"
CORRECTED_BUCKET = "corrected-ffic"
S3_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "test123minio"
MONGO_URI = "mongodb://mongodb:27017/"


# --------------------- Download/Upload Functions ---------------------
def download_fits_from_s3(s3_key: str) -> np.ndarray:
    """
    Download a FITS file from MinIO directly into memory and return its data.

    Args:
        s3_key (str): The key (filename) of the FITS file in the S3 bucket.

    Returns:
        np.ndarray: The raw image data extracted from the FITS file.
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    try:
        with io.BytesIO() as mem_file:
            s3_client.download_fileobj(RAW_BUCKET, s3_key+".fits", mem_file)
            mem_file.seek(0)
            # Open the FITS file from memory and extract the image data from the second HDU.
            with fits.open(mem_file) as hdul:
                raw_image = hdul[1].data
        print(f"Downloaded {s3_key} from bucket '{RAW_BUCKET}' into memory.")
        return raw_image
    except Exception as e:
        print(f"Error downloading {s3_key} from S3: {e}")
        raise

def upload_processed_image_to_s3(processed_image: np.ndarray, bucket: str, key: str) -> None:
    """
    Upload a processed image (NumPy array) to an S3 bucket as a binary file.

    Args:
        processed_image (np.ndarray): The processed image to upload.
        bucket (str): The target S3 bucket name.
        key (str): The key (filename) under which the image will be stored.
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    try:
        with io.BytesIO() as mem_file:
            # Save the NumPy array to the in-memory file.
            np.save(mem_file, processed_image)
            mem_file.seek(0)  # Reset file pointer to the beginning.
            # Upload the binary data to the specified S3 bucket.
            s3_client.upload_fileobj(mem_file, bucket, key)
            print(f"Uploaded processed image to bucket '{bucket}' with key '{key}'.")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        raise


# --------------------- Background Estimation Functions ---------------------
def clip_3sigma(tile: np.ndarray) -> np.ndarray:
    """
    Perform 3-sigma clipping on the input tile.

    Args:
        tile (np.ndarray): The input array (tile) to be clipped.

    Returns:
        np.ndarray: The array containing only values within 3 standard deviations from the median.
    """
    median_val = np.median(tile)
    std_dev = np.std(tile)
    return tile[np.abs(tile - median_val) < 3 * std_dev]

def tile_mode(tile: np.ndarray) -> float:
    """
    Compute the mode of a tile with a 3-sigma clipping.

    Args:
        tile (np.ndarray): The input array (tile) from which to compute the mode.

    Returns:
        float: The estimated mode value for the tile.
    """
    clipped = clip_3sigma(tile)
    if clipped.size and np.std(clipped) < 0.3 * np.median(clipped):
        return 2.5 * np.median(clipped) - 1.5 * np.mean(clipped)
    else:
        return np.median(tile)

def view_as_blocks_custom(arr: np.ndarray, block_shape: Tuple[int, int]) -> np.ndarray:
    """
    Reshape the array into non-overlapping blocks of the specified shape.

    Args:
        arr (np.ndarray): The input 2D array.
        block_shape (Tuple[int, int]): The shape (height, width) of each block.

    Returns:
        np.ndarray: A view of the array reshaped into blocks.
    """
    m, n = arr.shape
    a, b = block_shape
    new_shape = (m // a, a, n // b, b)
    return arr.reshape(new_shape).swapaxes(1, 2)

def estimate_square_background(image: np.ndarray, tile_size: int = 64) -> np.ndarray:
    """
    Estimate the background of the image using square tiles.

    Args:
        image (np.ndarray): The input image data.
        tile_size (int, optional): The size of the square tiles. Defaults to 64.

    Returns:
        np.ndarray: The estimated background as a 2D array.
    """
    blocks = view_as_blocks_custom(image, (tile_size, tile_size))
    vectorized_tile_mode = np.vectorize(tile_mode, signature='(m,n)->()')
    mode_grid = vectorized_tile_mode(blocks)
    mode_grid_smoothed = median_filter(mode_grid, size=3)
    ny, nx = mode_grid_smoothed.shape
    h, w = image.shape
    y_grid, x_grid = np.linspace(0, h, ny), np.linspace(0, w, nx)
    spline = RectBivariateSpline(y_grid, x_grid, mode_grid_smoothed)
    return spline(np.arange(h), np.arange(w))

def estimate_radial_background(image: np.ndarray, side: str, vertical: str,
                               start_radius: int = 2400, bin_width: int = 15) -> np.ndarray:
    """
    Estimate the radial background to remove corner glow.

    Args:
        image (np.ndarray): Input image data.
        side (str): Horizontal position ("left" or "right") of CCD relative to the camera
        vertical (str): Vertical position ("top" or "bottom") of CCD relative to the camera
        start_radius (int, optional): Starting radius for background estimation.
        bin_width (int, optional): Wdth of each radial bin.

    Returns:
        np.ndarray: The estimated radial background as a 2D array.
    """
    h, w = image.shape
    origin_y = (h - 1) if vertical == "top" else 0
    origin_x = (w - 1) if side == "left" else 0
    y_indices, x_indices = np.indices(image.shape)
    dist = np.sqrt((x_indices - origin_x)**2 + (y_indices - origin_y)**2)
    bins = np.arange(start_radius, np.max(dist) + bin_width, bin_width)
    radial_profile = np.array([
        tile_mode(image[(dist >= b) & (dist < b + bin_width)])
        if np.any((dist >= b) & (dist < b + bin_width)) else np.nan
        for b in bins[:-1]
    ])
    valid = ~np.isnan(radial_profile)
    interp_profile = np.interp(
        dist.flat,
        (bins[:-1][valid] + bin_width / 2.0),
        radial_profile[valid],
        left=radial_profile[valid][0] if valid.any() else np.median(image),
        right=radial_profile[valid][-1] if valid.any() else np.median(image)
    )
    return interp_profile.reshape(image.shape)

def iterative_background_estimation(image: np.ndarray, side: str, vertical: str,
                                    iterations: int = 3, tile_size: int = 64,
                                    start_radius: int = 2400, bin_width: int = 15) -> np.ndarray:
    """
    Iteratively estimate the background by combining square and radial estimates.

    Args:
        image (np.ndarray): Input image data
        side (str): Horizontal position ("left" or "right") of CCD relative to the camera
        vertical (str, optional): Vertical position ("up" or "down") of CCD relative to the camera
        iterations (int, optional): Number of iterations to refine the background estimate
        tile_size (int, optional): Size of the square tiles for background estimation
        start_radius (int, optional): Starting radius for radial background estimation
        bin_width (int, optional): Width of each radial bin

    Returns:
        np.ndarray: The combined estimated background as a 2D array.
    """
    def update(b_tuple: Tuple[np.ndarray, np.ndarray], _: int) -> Tuple[np.ndarray, np.ndarray]:
        b_square, _ = b_tuple
        new_b_radial = estimate_radial_background(
            image - b_square, side, vertical=vertical,
            start_radius=start_radius, bin_width=bin_width)
        new_b_square = estimate_square_background(image - new_b_radial, tile_size)
        gc.collect()
        return (new_b_square, new_b_radial)

    initial = (
        estimate_square_background(image, tile_size),
        estimate_radial_background(image, side, vertical=vertical,
                                   start_radius=start_radius, bin_width=bin_width)
    )
    b_square, b_radial = reduce(update, range(iterations), initial)
    return b_square + b_radial

def process_image(image: np.ndarray, side: str, vertical: str = "top") -> np.ndarray:
    """
    Process the input image by cropping and subtracting the estimated background.

    Args:
        image (np.ndarray): The raw input image data.
        side (str): Horizontal position ("left" or "right") of CCD relative to the camera
        vertical (str, optional): Vertical position ("up" or "down") of CCD relative to the camera

    Returns:
        np.ndarray: The processed image with the background subtracted.
    """
    # Crop the image to remove the buffer rows and columns (see TESS handbook page 24)
    # Estimate and subtract the background from the cropped image.
    background = iterative_background_estimation(image, side, vertical=vertical)
    processed_image = image - background
    return processed_image

def get_ccd_position(ccd: int) -> Tuple[str, str]:
    """
    Map the CCD number to the corresponding side and vertical parameters for background estimation.

    Args:
        ccd (int): The CCD identifier (expected values: 1, 2, 3, or 4).

    Returns:
        Tuple[str, str]: A tuple containing the horizontal and vertical parameters.

    Raises:
        ValueError: If the CCD number is not one of the expected values.
    """
    if ccd == 1:
        return "left", "top"
    elif ccd == 2:
        return "right", "top"
    elif ccd == 3:
        return "left", "bottom"
    elif ccd == 4:
        return "right", "bottom"
    else:
        raise ValueError("Invalid CCD value; must be 1, 2, 3, or 4.")

def process_single_ffi(doc: Dict) -> str:
    """
    Process a single FFI document.

    This function:
      - Validates and parses the metadata from the document.
      - Downloads the corresponding raw FITS image.
      - Determines background estimation parameters based on CCD.
      - Processes the image (cropping, background subtraction, corner glow removal).
      - Uploads the processed image to the designated S3 bucket.

    Args:
        doc (Dict): A dictionary containing FFI metadata from MongoDB.

    Returns:
        str: A status message indicating success or failure for the processed file.
    """
    s3_key: str = doc.get("filename")
    ffi_metadata = doc.get("secondary_header")
    ccd: int = ffi_metadata.get("CCD")

    try:
        raw_image = download_fits_from_s3(s3_key)
    except ClientError as e:
        return f"Error downloading {s3_key}: {e}"

    side, vertical = get_ccd_position(ccd)

    raw_image = raw_image[:-30, 44:-44] # Temporary crop buffer rows for stats
    processed_image = process_image(raw_image, side, vertical)
    processed_image = np.pad(processed_image, ((0, 30), (44, 44)))
    key = s3_key + ".npy"

    try:
        upload_processed_image_to_s3(processed_image, CORRECTED_BUCKET, key)
        return f"Processed {s3_key} successfully."
    except ClientError as e:
        return f"Error uploading {s3_key}: {e}"

def main(upload_time_threshold: float):
    """
    Run the image processing pipeline in parallel for documents with an upload_time
    greater than the specified threshold. After processing, delete the corresponding
    staging files from S3 and remove the documents from the metadata collection.
    """
    client = MongoClient(MONGO_URI)
    db = client["fits_metadata"]
    metadata_collection = db["metadata"]

    query = {"upload_time": {"$gte": upload_time_threshold}}
    ffis: List[Dict] = list(metadata_collection.find(query))

    if not ffis:
        print("No FFI metadata found in MongoDB after the given upload_time threshold.")
        return

    results: List[str] = []
    with ProcessPoolExecutor() as executor:
        future_to_doc = {executor.submit(process_single_ffi, doc): doc for doc in ffis}
        for future in as_completed(future_to_doc):
            result = future.result()
            print(result)
            results.append(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process raw TESS FFI images"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2018-01-01",
        help="The start date for processing FFI images (ISO format).",
    )
    args = parser.parse_args()

    # Convert the provided ISO datetime string to a UNIX timestamp.
    dt = datetime.fromisoformat(args.start_date)
    threshold = dt.timestamp()

    main(threshold)
