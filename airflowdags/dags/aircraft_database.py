import logging
from datetime import timedelta

import boto3
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Define constants
AIRCRAFT_DATABASE_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft-database-complete-2022-09.csv.gz"
AWS_BUCKET = "bdi-aircraft-alexi"  # Your S3 bucket name
S3_PREFIX = "raw/"  # Folder path inside S3 (used "raw/" based on your existing structure)
S3_KEY = "aircraft-database-complete-2022-09.csv.gz"  # The file name in S3

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='download_aircraft_database_dag',
    description='DAG to download and upload the Aircraft Database from GitHub',
    schedule_interval=None,  # Set to None for manual trigger
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=['download', 'aircraft', 'database'],
)

@task()
def download_and_upload():
    """Download the Aircraft Database (CSV file) and upload to S3"""
    # Step 1: Download the aircraft database from the GitHub URL
    try:
        logging.info(f"Attempting to download the Aircraft Database from {AIRCRAFT_DATABASE_URL}")
        response = requests.get(AIRCRAFT_DATABASE_URL)
        response.raise_for_status()  # Will raise an exception for 4xx or 5xx errors
        logging.info(f"Successfully fetched the Aircraft Database from {AIRCRAFT_DATABASE_URL}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download the database: {e}")
        raise

    # Step 2: Upload the file to S3
    try:
        logging.info(f"Starting upload to S3 bucket {AWS_BUCKET}, at {S3_PREFIX + S3_KEY}")

        # Initialize the S3 client
        s3_client = boto3.client('s3')

        # Upload the raw file content directly to S3
        s3_client.put_object(
            Bucket=AWS_BUCKET,
            Key=S3_PREFIX + S3_KEY,
            Body=response.content,  # Raw content of the .csv.gz file
            ContentType='application/gzip'  # Content type for gzipped file
        )

        logging.info(f"Successfully uploaded the aircraft database to s3://{AWS_BUCKET}/{S3_PREFIX + S3_KEY}")

    except boto3.exceptions.S3UploadFailedError as e:
        logging.error(f"Failed to upload the data to S3 due to upload error: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to upload the data to S3: {e}")
        raise

# Make sure the task is added to the DAG context
with dag:
    download_and_upload()
