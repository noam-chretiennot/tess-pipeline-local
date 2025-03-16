"""
Module for downloading FITS files and uploading them to S3
"""

import os
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
import requests
import boto3
from botocore.exceptions import BotoCoreError
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import TransferConfig


def download_fits():
    """Download FITS files from the given URLs."""
    input_file = "./tess_ffic_sector_1_tiny_urls.txt"  # File containing URLs
    download_folder = "./download"
    log_file = "./log.txt"

    # Create download folder if it doesn't exist
    os.makedirs(download_folder, exist_ok=True)

    file_urls = []
    # Open log file
    with open(log_file, "w", encoding='utf-8') as log:
        with open(input_file, "r", encoding='utf-8') as infile:
            for url in infile:
                url = url.strip()
                if not url:
                    continue

                filename = os.path.join(download_folder, url.split("/")[-1])

                # Check if file is present in the download folder
                if os.path.exists(filename):
                    log.write(f"SKIP: File already exists {filename}\n")
                    file_urls.append(filename)
                else:
                    response = None
                    cpt = 0
                    sleep_time = 1
                    while response is None or cpt == 5:
                        try:
                            response = requests.get(url, stream=True)
                            response.raise_for_status()

                            with open(filename, "wb") as file:
                                for chunk in response.iter_content(chunk_size=8192):
                                    file.write(chunk)

                            log.write(f"SUCCESS: Downloaded {url} -> {filename}\n")
                            file_urls.append(filename)
                        except requests.RequestException as e:
                            cpt += 1
                            sleep_time *= 2
                            time.sleep(sleep_time)
                            if cpt == 5:
                                log.write(f"ERROR: Failed to download {url} - {e}\n")
                                print(f"Failed: {url}")

    print("Download completed. Check log.txt for details.")
    return file_urls


def upload_to_s3(endpoint_url, file_urls):
    """Upload FITS files to S3 with optimized multipart upload settings."""
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id="minio",
        aws_secret_access_key="test123minio"
    )

    bucket_name = "raw-ffi"

    # Optimize multipart uploads for 40MB files
    transfer_config = TransferConfig(
        multipart_threshold=10 * 1024 * 1024,
        multipart_chunksize=10 * 1024 * 1024,
        max_concurrency=4,
        use_threads=True
    )

    # Use ThreadPoolExecutor for parallel uploads
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for file in file_urls:
            futures.append(
                executor.submit(upload_file, s3_client, file, bucket_name, transfer_config)
            )

        # Wait for all futures to complete
        for future in futures:
            future.result()

    print("All files uploaded successfully.")


def upload_file(s3_client, file, bucket_name, transfer_config):
    """Helper function to upload a single file."""
    try:
        s3_client.upload_file(
            file,
            bucket_name,
            os.path.basename(file),
            Config=transfer_config
        )
        print(f"Uploaded: {file} -> s3://{bucket_name}/{os.path.basename(file)}")
    except (S3UploadFailedError, BotoCoreError) as e:
        print(f"Error uploading {file}: {e}")


def main():
    """Parse arguments and manage the download and upload process."""
    parser = argparse.ArgumentParser(description="Download and process FITS files")
    parser.add_argument('--endpoint-url', type=str,
                        default='http://localhost:9000',
                        help='URL of the S3 endpoint (LocalStack)')

    args = parser.parse_args()

    print("Downloading data...")
    file_urls = download_fits()
    print("Data downloaded and organized.")

    print("Uploading files...")
    upload_to_s3(args.endpoint_url, file_urls)
    print("Processing completed.")


if __name__ == "__main__":
    main()
