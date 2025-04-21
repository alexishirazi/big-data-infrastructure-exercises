import os
from datetime import datetime
from io import BytesIO
from urllib.parse import urljoin

import boto3
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.log.logging_mixin import LoggingMixin
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/"
AWS_BUCKET = "bdi-aircraft-alexi"

logger = LoggingMixin().log

@dag(
    dag_id="download_readsb_data_dag",
    description="Download 100 readsb JSON.gz files from a specific day and upload to S3 (RAW)",
    start_date=datetime(2023, 11, 1),
    schedule_interval=None,
    catchup=True,
    tags=["readsb", "download", "raw"],
    params={"limit": 100},
)
def download_readsb_data():

    @task()
    def download_for_day(limit: int = 100):
        context = get_current_context()
        execution_date = context["execution_date"]

        # Use default session (credentials from env)
        session = boto3.Session()
        s3_client = session.client("s3")

        day_str = execution_date.strftime("%Y/%m/%d")
        s3_prefix = f"raw/day={execution_date.strftime('%Y%m%d')}/"
        url = urljoin(BASE_URL, day_str + "/")

        logger.info(f"Fetching file list from {url}")
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch file list: {e}")
            raise

        soup = BeautifulSoup(response.text, "html.parser")
        files = [
            a["href"] for a in soup.find_all("a")
            if a["href"].endswith(".json.gz")
        ][:limit]

        for filename in files:
            s3_key = os.path.join(s3_prefix, filename).replace("\\", "/")
            file_url = urljoin(url, filename)

            # Check if file already exists in S3
            try:
                s3_client.head_object(Bucket=AWS_BUCKET, Key=s3_key)
                logger.info(f"File {s3_key} already exists in S3. Skipping.")
                continue
            except s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey':
                    logger.error(f"Error checking file existence in S3: {e}")
                    raise

            try:
                logger.info(f"Downloading {file_url}")
                r = requests.get(file_url, stream=True)
                r.raise_for_status()

                s3_client.upload_fileobj(BytesIO(r.content), AWS_BUCKET, s3_key)
                logger.info(f"Uploaded {s3_key} to S3")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error downloading {filename}: {e}")
            except Exception as e:
                logger.error(f"Error uploading {filename} to S3: {e}")

    download_for_day()

dag = download_readsb_data()
