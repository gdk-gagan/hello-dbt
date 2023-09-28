"""Credits: DE Zoomcamp github - https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_3_data_warehouse/extras """

import os
import requests
import pandas as pd
from google.cloud import storage
from time import time

"""
Pre-reqs: 
1. Install libraries 
    pip install pandas pyarrow google-cloud-storage
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key. Check gcloud config set.
    export GOOGLE_APPLICATION_CREDENTIALS="/Users/gdk/google-cloud-sdk/dtc-de-2023.json"
3. Create a bucket, 'de-week4-data-lake' from the Google Cloud UI console
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
    export GCP_GCS_BUCKET='de-week4-data-lake'
4. Authenticate with google cloud - 
    gcloud auth application-default login
"""

# path to download data locally
data_dir = "../data"
# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "de-week4-data-lake")

def upload_to_gcs(bucket: storage.Client, object_name: str, local_file: str) -> None:
    """ Upload blob from local directory to GCS using gcs client
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python

    Args:
        bucket (storage.Client): GCS client object
        object_name (str): File name to upload
        local_file (str): Local path for the file
    """
    
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def make_service_dir(service: str) -> None:
    """Create a local directory for each service 

    Args:
        service (str): Taxi service (yellow/green/fhv)
    """
    
    service_dir = f"{data_dir}/{service}"
    try:
        if not os.path.exists(service_dir):
            os.mkdir(service_dir)
        print(f"\n\nCreated directory for service {service}")
    except Exception as e:
        print(f"Failed to create service dir with exception {e}")
        raise

def web_to_gcs(year: int, service: str) -> None:
    """Upload NY taxi data for the given year (all months) and given service (yellow, green etc.)

    Args:
        year (int): Year
        service (str): Taxi service like yellow, green, fhv
    """

    make_service_dir(service)

    start_time = time()

    for i in range(12):
        start = time()
        print(f"Ingesting data for {service}{year} - month: {i}")
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(f"{data_dir}/{service}/{file_name}", 'wb').write(r.content)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(f"{data_dir}/{service}/{file_name}", compression='gzip')
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(f"{data_dir}/{service}/{file_name}", engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", f"{data_dir}/{service}/{file_name}")
        print(f"GCS: {service}/{file_name}")

        end = time()
        print(f"Inserted month {i} for {service} {year} in {end-start} seconds")
    
    end_time = time()
    print(f"\Ingested {service} {year} data to GCS. Took {(end_time-start_time)/60} min")


if __name__ == '__main__':

    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
        print(f"Created data directory at {data_dir}")

    web_to_gcs('2019', 'green')
    web_to_gcs('2020', 'green')
    web_to_gcs('2019', 'yellow')
    web_to_gcs('2020', 'yellow')
    web_to_gcs('2019', 'fhv')