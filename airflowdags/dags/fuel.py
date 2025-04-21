import logging
from datetime import datetime
from io import BytesIO

import boto3
import requests
from airflow.decorators import dag, task  # Make sure to import 'dag' here

# Configuration
BASE_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
AWS_BUCKET = "bdi-aircraft-alexi"

# Initialize logger
logger = logging.getLogger(__name__)

@dag(
    dag_id="download_fuel_consumption_data_dag",
    description="Download aircraft fuel consumption data from GitHub and upload it to S3",
    start_date=datetime(2023, 11, 1),
    schedule_interval=None,
    catchup=False,
    tags=["fuel_consumption", "download", "s3"]
)
def download_fuel_consumption_data():

    @task()
    def download_and_upload_fuel_data():
        try:
            # Fetch the fuel consumption data from the provided GitHub URL
            logger.info(f"Fetching fuel consumption data from {BASE_URL}")
            response = requests.get(BASE_URL)
            response.raise_for_status()  # This will raise an error for a 404 or any other error

            # Upload the fuel data to S3 (in the same raw bucket as your other data)
            s3 = boto3.client("s3")
            s3_key = "raw/fuel_consumption_data/aircraft_type_fuel_consumption_rates.json"

            # Convert the data to bytes and upload
            s3.upload_fileobj(BytesIO(response.content), AWS_BUCKET, s3_key)
            logger.info(f"Fuel consumption data uploaded successfully to S3 at {s3_key}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download fuel consumption data: {e}")
            raise

    download_and_upload_fuel_data()

# Instantiate the DAG
dag = download_fuel_consumption_data()
