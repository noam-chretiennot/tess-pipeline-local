import os
import requests
import time

input_file = "dataset/tess_ffic_sector_1_mini_urls.txt"  # File containing URLs
download_folder = "dataset/download"
log_file = "dataset/log.txt"

# Create download folder if it doesn't exist
os.makedirs(download_folder, exist_ok=True)

# Open log file
with open(log_file, "w") as log:
    with open(input_file, "r") as infile:
        for url in infile:
            url = url.strip()
            if not url:
                continue
            
            filename = os.path.join(download_folder, url.split("/")[-1])
            
            response = None
            cpt = 0
            sleep_time = 1
            while response is None or cpt==5:
                try:
                    response = requests.get(url, stream=True)
                    response.raise_for_status()
                    
                    with open(filename, "wb") as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            file.write(chunk)
                    
                    log.write(f"SUCCESS: Downloaded {url} -> {filename}\n")
                except requests.RequestException as e:
                    cpt += 1
                    sleep_time *= 2
                    time.sleep(sleep_time)
                    if cpt == 5:                
                        log.write(f"ERROR: Failed to download {url} - {e}\n")
                        print(f"Failed: {url}")

print("Download completed. Check log.txt for details.")
