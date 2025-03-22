"""
Streamlit API DataViz Dashboard

This dashboard retrieves data from an API and visualizes it.
It includes the following sections:
    - Health: Display basic API health info.
    - Statistics: Show bucket and MongoDB collection metrics.
    - Raw Bucket: Filter and directly plot raw image files.
    - Staging Bucket: Filter and directly plot staging image files.
    - Cluster Data: Plot light curve and aperture data for a randomly fetched cluster.
"""

import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from astropy.time import Time
from astropy.io import fits
import numpy as np
import os
import io

# --------------------- config -----------------------------
BASE_URL = os.environ.get("API_URL", "http://localhost:8000")

st.title("API Test Dashboard")

# --------------------- Health Section ---------------------
st.header("Health")
try:
    # Request health information from the API
    health_response = requests.get(f"{BASE_URL}/health").json()
    st.json(health_response)
except Exception as e:
    st.error(f"Error fetching health info: {e}")

# --------------------- Statistics Section ------------------
st.header("Statistics")
try:
    # Request overall statistics from the API
    stats_response = requests.get(f"{BASE_URL}/stats").json()

    # Process and display bucket metrics
    bucket_data = []
    for bucket, metrics in stats_response.get("buckets", {}).items():
        bucket_data.append({
            "Bucket": bucket,
            "Object Count": metrics.get("object_count", 0),
            "Total Size (bytes)": metrics.get("total_size", 0)
        })
    if bucket_data:
        df_buckets = pd.DataFrame(bucket_data)
        st.subheader("Bucket Metrics")
        st.dataframe(df_buckets)
    else:
        st.write("No bucket metrics available.")

    # Process and display MongoDB collection metrics
    collection_data = []
    collections = stats_response.get("collections", {})
    for db_name, coll_dict in collections.items():
        for coll_name, count in coll_dict.items():
            collection_data.append({
                "Database": db_name,
                "Collection": coll_name,
                "Document Count": count
            })
    if collection_data:
        df_collections = pd.DataFrame(collection_data)
        st.subheader("MongoDB Collection Metrics")
        st.dataframe(df_collections)
    else:
        st.write("No collection metrics available.")
except Exception as e:
    st.error(f"Error fetching statistics: {e}")

# --------------------- Metadata Values Section -------------
try:
    # Request distinct metadata values for filtering selectors
    meta_values = requests.get(f"{BASE_URL}/metadata/values").json()
    # Build selection lists with "All" option included
    cameras = ["All"] + sorted(meta_values.get("CAMERA", []))
    ccds = ["All"] + sorted(meta_values.get("CCD", []))
    date_obs = ["All"] + sorted(meta_values.get("date_obs", []))
except Exception as e:
    st.error(f"Error fetching metadata values: {e}")
    cameras, ccds, date_obs = ["All"], ["All"], ["All"]

# --------------------- Helper Functions --------------------
def display_file_image(bucket, key):
    """
    Download and display an image file from the given bucket/key.
    Supports FITS and NPY files. For other file types, the image is shown directly.
    """
    try:
        download_url = f"{BASE_URL}/download?bucket={bucket}&key={key}"
        response = requests.get(download_url)
        response.raise_for_status()  # Ensure a successful response

        # Determine file extension to choose appropriate loader
        ext = key.split('.')[-1].lower()
        if ext == "fits":
            # Open FITS file from downloaded content
            with fits.open(io.BytesIO(response.content)) as hdul:
                # Prefer data from hdul[1] if available; otherwise, fallback to hdul[0]
                if len(hdul) > 1 and hdul[1].data is not None:
                    data = hdul[1].data
                else:
                    data = hdul[0].data
            fig, ax = plt.subplots()
            # Plot the image using percentile-based scaling for contrast enhancement
            cax = ax.imshow(data, vmin=np.percentile(data, 4),
                            vmax=np.percentile(data, 98), origin="lower")
            ax.set_title(key)
            fig.colorbar(cax)
            st.pyplot(fig)
        elif ext == "npy":
            # Load Numpy array from downloaded content
            data = np.load(io.BytesIO(response.content))
            fig, ax = plt.subplots()
            cax = ax.imshow(data, vmin=np.percentile(data, 4),
                            vmax=np.percentile(data, 98), origin="lower")
            ax.set_title(key)
            fig.colorbar(cax)
            st.pyplot(fig)
        else:
            # Display any other image file directly
            st.image(response.content, caption=key)
    except Exception as e:
        st.error(f"Error plotting file {key}: {e}")

# --------------------- Raw ---------------------
st.header("Raw Bucket")
with st.form("raw_filter_form"):
    # Create selectors for filtering raw bucket data
    selected_date = st.selectbox("OBS_DATE", date_obs)
    selected_camera = st.selectbox("CAMERA", cameras)
    selected_ccd = st.selectbox("CCD", ccds)
    submitted = st.form_submit_button("Filter Raw")
    if submitted:
        try:
            # Build query parameters based on user selection
            params = {}
            if selected_date != "All":
                params["obs_date"] = selected_date
            if selected_camera != "All":
                params["camera"] = selected_camera
            if selected_ccd != "All":
                params["ccd"] = selected_ccd

            # Request raw bucket data
            raw_response = requests.get(f"{BASE_URL}/raw", params=params).json()
            bucket = raw_response.get("bucket")
            st.write(f"Bucket: {bucket}")
            objects = raw_response.get("objects", [])
            if objects:
                # Create a DataFrame with object keys and sizes for display
                df_raw = pd.DataFrame([{"Key": obj["Key"], "Size": obj["Size"]} for obj in objects])
                st.dataframe(df_raw)
                st.subheader("Raw Files - Direct Plotting")
                # Loop through each object to display its corresponding image
                for obj in objects:
                    key = obj["Key"]
                    st.write(f"Plot for: {key}")
                    display_file_image(bucket, key)
            else:
                st.write("No objects found.")
        except Exception as e:
            st.error(f"Error fetching raw bucket data: {e}")

# -----------------------------------------------------------------------------
# Staging Bucket Filtering Section with Direct Plotting
# -----------------------------------------------------------------------------
st.header("Staging Bucket")
with st.form("staging_filter_form"):
    # Create selectors for staging bucket filtering with distinct keys
    selected_date_staging = st.selectbox("OBS_DATE (Staging)", date_obs, key="date_obs_staging")
    selected_camera_staging = st.selectbox("CAMERA (Staging)", cameras, key="camera_staging")
    selected_ccd_staging = st.selectbox("CCD (Staging)", ccds, key="ccd_staging")
    submitted_staging = st.form_submit_button("Filter Staging")
    if submitted_staging:
        try:
            # Build query parameters for the staging endpoint
            params = {}
            if selected_date_staging != "All":
                params["obs_date"] = selected_date_staging
            if selected_camera_staging != "All":
                params["camera"] = selected_camera_staging
            if selected_ccd_staging != "All":
                params["ccd"] = selected_ccd_staging

            # Request staging bucket data
            staging_response = requests.get(f"{BASE_URL}/staging", params=params).json()
            bucket = staging_response.get("bucket")
            st.write(f"Bucket: {bucket}")
            objects = staging_response.get("objects", [])
            if objects:
                # Display object details in a DataFrame
                df_staging = pd.DataFrame([{"Key": obj["Key"], "Size": obj["Size"]}\
                                           for obj in objects])
                st.dataframe(df_staging)
                st.subheader("Staging Files - Direct Plotting")
                # Loop through each staging object to display its image
                for obj in objects:
                    key = obj["Key"]
                    st.write(f"Plot for: {key}")
                    display_file_image(bucket, key)
            else:
                st.write("No objects found.")
        except Exception as e:
            st.error(f"Error fetching staging bucket data: {e}")

# ------------------------ Light Curve & Aperture ------------------------
st.header("Star Data (Light Curve & Aperture)")
if st.button("Fetch Random Cluster Data"):
    try:
        # Request a random cluster data document from the API
        cluster_data = requests.get(f"{BASE_URL}/curated").json()
        cluster_label = cluster_data.get("cluster_label")
        lc = cluster_data.get("light_curve", {})
        ap = cluster_data.get("aperture", {})

        # --- Plot Light Curve ---
        timestamps = lc.get("timestamps", [])
        cluster_fluxes = lc.get("cluster_fluxes", [])
        mask_fluxes = lc.get("mask_fluxes", [])
        if timestamps and cluster_fluxes and mask_fluxes:
            # Convert timestamps to datetime objects using Astropy's Time module
            dt_list = [Time(ts).datetime for ts in timestamps]
            fig, ax1 = plt.subplots(figsize=(10, 6))
            ax1.set_xlabel("Observation Timestamp")
            ax1.set_ylabel("In aperture Flux", color="tab:blue")
            # Plot the cluster flux as a line with markers
            l1 = ax1.plot(dt_list, cluster_fluxes,
                          marker="o", linestyle="-", color="tab:blue",
                          label="Cluster Flux")
            ax1.tick_params(axis="y", labelcolor="tab:blue")
            # Format the x-axis for dates
            ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
            plt.xticks(rotation=45)
            # Create a secondary y-axis for mask flux
            ax2 = ax1.twinx()
            ax2.set_ylabel("Mask Flux", color="tab:red")
            l2 = ax2.plot(dt_list, mask_fluxes,
                          marker="x", linestyle="-", color="tab:red",
                          label="Mask Flux")
            ax2.tick_params(axis="y", labelcolor="tab:red")

            lines = l1 + l2
            labels = [line.get_label() for line in lines]
            ax1.legend(lines, labels, loc="upper left")
            ax1.set_title(f"Light Curve for Cluster Label: {cluster_label}")
            st.pyplot(fig)
        else:
            st.write("Insufficient light curve data.")

        # --- Plot Aperture ---
        pixels = ap.get("pixels", [])
        centroid = ap.get("centroid", None)
        if pixels:
            try:
                xs, ys = zip(*pixels)
                fig2, ax = plt.subplots(figsize=(6, 6))
                ax.scatter(xs, ys, label="Pixels")
                if centroid:
                    ax.scatter(centroid[0], centroid[1],
                               color="red", marker="x", s=100,
                               label="Centroid")
                ax.set_title(f"Aperture for Cluster Label: {cluster_label}")
                ax.set_xlabel("X coordinate")
                ax.set_ylabel("Y coordinate")
                ax.legend()
                ax.grid(True)
                st.pyplot(fig2)
            except Exception as e:
                st.error(f"Error processing aperture data: {e}")
        else:
            st.write("No aperture data available.")
    except Exception as e:
        st.error(f"Error fetching cluster data: {e}")
