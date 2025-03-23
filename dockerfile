FROM apache/airflow:2.7.1

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Create directories
RUN mkdir -p /opt/airflow/build /opt/airflow/data/raw /opt/airflow/scripts
RUN chown -R airflow:root /opt/airflow/build /opt/airflow/data/raw /opt/airflow/scripts

USER airflow

# Copy requirement & install
COPY requirements.txt /opt/airflow/build/reqs.txt
RUN pip install -r /opt/airflow/build/reqs.txt
