"""
Module for downloading FITS files and injecting them through the API.
"""

import os
import time
import requests
from requests.exceptions import RequestException

def download_fits() -> list:
    """
    Download FITS files from the URLs listed in an input file.
    
    Returns:
        List of local file paths for the successfully downloaded files.
    """
    input_file = "/opt/airflow/build/tess_ffic_sector_1_tiny_urls.txt"  # File containing URLs
    download_folder = "/opt/airflow/build/download"
    log_file = "/opt/airflow/build/log.txt"

    # Create download folder if it doesn't exist.
    os.makedirs(download_folder, exist_ok=True)

    file_paths = []
    with open(log_file, "w", encoding='utf-8') as log:
        with open(input_file, "r", encoding='utf-8') as infile:
            for url in infile:
                url = url.strip()
                if not url:
                    continue

                filename = os.path.join(download_folder, url.split("/")[-1])
                # If file exists, skip downloading.
                if os.path.exists(filename):
                    log.write(f"SKIP: File already exists {filename}\n")
                    file_paths.append(filename)
                    continue

                attempt = 0
                sleep_time = 1
                while attempt < 5:
                    try:
                        response = requests.get(url, stream=True)
                        response.raise_for_status()
                        with open(filename, "wb") as file:
                            for chunk in response.iter_content(chunk_size=8192):
                                file.write(chunk)
                        log.write(f"SUCCESS: Downloaded {url} -> {filename}\n")
                        file_paths.append(filename)
                        break  # Download successful, exit loop.
                    except RequestException as e:
                        attempt += 1
                        sleep_time *= 2
                        time.sleep(sleep_time)
                        if attempt == 5:
                            log.write(f"ERROR: Failed to download {url} - {e}\n")
                            print(f"Failed to download: {url}")
                            raise e

    print("Download completed. Check log.txt for details.")
    return file_paths

def inject_file(file_path: str) -> None:
    """
    Inject a single FITS file to the API's inject endpoint.
    """
    endpoint = f"http://api:8000/inject"
    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f, "application/octet-stream")}
            response = requests.post(endpoint, files=files)
            response.raise_for_status()
            print(f"Successfully injected {file_path} to API.")
    except Exception as e:
        print(f"Error injecting {file_path}: {e}")

def main():
    """
    Parse arguments, download FITS files, and inject them to the API sequentially.
    """
    print("Downloading FITS files...")
    file_paths = download_fits()
    if not file_paths:
        print("No files downloaded. Exiting.")
        return

    print("Injecting files to API...")
    for file_path in file_paths:
        inject_file(file_path)
    print("All files processed.")

if __name__ == "__main__":
    main()
