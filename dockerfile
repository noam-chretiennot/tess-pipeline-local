FROM apache/airflow:2.7.1

USER root

# Installation des dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Création des dossiers nécessaires
RUN mkdir -p /opt/airflow/build /opt/airflow/data/raw /opt/airflow/scripts
RUN chown -R airflow:root /opt/airflow/build /opt/airflow/data/raw /opt/airflow/scripts

USER airflow

# Copie des requirements et installation
COPY build/reqs.txt /opt/airflow/build/reqs.txt
RUN pip install -r /opt/airflow/build/reqs.txt

# Copie des scripts pour les rendre utilisables par Airflow
COPY build/*.py /opt/airflow/build/
COPY src/*.py /opt/airflow/scripts/