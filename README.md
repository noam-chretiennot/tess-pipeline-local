# Reproducing TASSOC image processing pipeline

## Project Overview
This project is a reproduction of the TASSOC pipeline for processing Callibrated Full-Frame Images images of the TESS mission

![steps](https://github.com/user-attachments/assets/b3a96f2d-0437-4789-b1f9-30b057f4acb5)


## Architecture
![Architecture](https://github.com/user-attachments/assets/27ac3ce6-276c-49f0-81f8-be1e4d9da19c)

### Technologies
- **Image Storage**: MinIO
- **Metadata, Aperture and Flux storage**: MongoDB
- **API Framework**: FastAPI
- **GUI**: Streamlit
- **Orcherstration**: Airflow
- **Containerization**: Docker (nothing runs outside docker)

## Transformation Scripts
1 Script is implemented in the Download pipeline
`unpack_to_raw.py` : downloads files and inputs them to `/injest`, caches files for future use (7Go for mini dataset, 86Go for small dataset)

Three scripts are implemented in the ETL pipeline :
- **Raw to Staging**
`preprocess_photometric.py` : Sky Background Substraction and Corner Glow removal

- **Staging to Staging**
`generate_apertures.py` : Fine Grain Clusterization of stars to find the apertures

- **Staging to Curated**
`generate_astroseismic_signal.py` : Gathers flux curve for each aperture


## API Gateway
Implemented endpoints:
- `/inject`: Access raw FITS data.
- `/raw` and `staging`: Query Image Path on OBS_DATE, CCD and CAMERA number
- `/download` : download a specific file from s3
- `/curated`: Access star apertures and light curve data.
- `/health`: Health checks for system services.
- `/stats`: Metrics on bucket and database usage.


## Installation
### Prerequisites
- Docker

### Building & Running the Project
```bash
git clone https://github.com/noam-chretiennot/tess-pipeline-local.git
cd tess-pipeline-local
docker-compose up -d
```
Access GUI : http://localhost:8080/

Execute Scripts : http://localhost:8081/

by default (do not use for production) :

    - id : airflow
    - password : airflow

# Optimisation
Each script only has a fast version, as the non-optimised version of the POC were just too slow to test them in prod and now it can't be optimised by 30% since all the overhead left is from MinIO.

Examples of optimisation used include :
- using parallelisation (impact : not accurately measured)
- process image in patches during watershed : O(N²) -> O(N)
- not computing refinement with watershed for clusters < 10 pixels (can't be divided into 2 stars)
- use searchsorted to apply the reapply the clusters labels after watershed : O(N) -> O(log(N))
- use indexes in mongodb

Future Optimisation should focus on better parallelization with different nodes. However, the setup for distributed Minio and distributed Dask is different than the setup for single node testing.

## Documentation
pipeline : https://iopscience.iop.org/article/10.3847/1538-3881/ac09f1/pdf

FITS image details : https://archive.stsci.edu/missions/tess/doc/TESS_Instrument_Handbook_v0.1.pdf

FITS metadata details : https://archive.stsci.edu/missions/tess/doc/EXP-TESS-ARC-ICD-TM-0014.pdf#page=18

---

*(Datalakes Project M2 - 2025)*
