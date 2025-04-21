import os
from io import BytesIO
from urllib.parse import urljoin

import boto3
import requests
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.dates import datetime
from bs4 import BeautifulSoup

# Configuración
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/"
AWS_BUCKET = "bdi-aircraft-alexi"  # <- Tu bucket real

# Inicializar cliente S3
session = boto3.Session()
s3_client = session.client("s3")

@dag(
    dag_id="download_readsb_data_dag",
    description="Download 100 readsb JSON.gz files from a specific day and upload to S3 (RAW)",
    start_date=datetime(2023, 11, 1),
    schedule_interval=None,  # Manual run
    catchup=True,
    tags=["readsb", "download", "raw"],
    params={"limit": Param(100, type="integer", description="File limit per day")},
)
def download_readsb_data():

    @task()
    def download_for_day(execution_date=None, limit: int = 100):
        """Download files from a given day and upload to S3"""
        if execution_date is None:
            raise ValueError("execution_date is required")

        day_str = execution_date.strftime("%Y/%m/%d")
        s3_prefix = f"raw/day={execution_date.strftime('%Y%m%d')}/"
        url = urljoin(BASE_URL, day_str + "/")

        print(f"Fetching file list from {url}")
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        files = [
            a["href"] for a in soup.find_all("a")
            if a["href"].endswith(".json.gz")
        ][:limit]

        for filename in files:
            s3_key = os.path.join(s3_prefix, filename).replace("\\", "/")
            file_url = urljoin(url, filename)

            # Check if file already exists (idempotency)
            try:
                s3_client.head_object(Bucket=AWS_BUCKET, Key=s3_key)
                print(f"File {s3_key} already exists in S3. Skipping.")
                continue
            except s3_client.exceptions.ClientError:
                pass

            print(f"Downloading {file_url}")
            r = requests.get(file_url, stream=True)
            if r.status_code == 200:
                s3_client.upload_fileobj(BytesIO(r.content), AWS_BUCKET, s3_key)
                print(f"Uploaded {s3_key} to S3")

    download_for_day()

dag = download_readsb_data()
