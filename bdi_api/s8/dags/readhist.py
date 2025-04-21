from datetime import datetime, timedelta

import boto3
import psycopg2
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator


# Function to get AWS credentials from the Airflow connection
def get_aws_credentials():
    """Retrieve AWS credentials from Airflow connection."""
    conn_id = 'aws_default'  # Connection ID for AWS
    connection = BaseHook.get_connection(conn_id)
    aws_access_key = connection.login
    aws_secret_key = connection.password
    return aws_access_key, aws_secret_key

# Function to download files from S3 (using boto3 and AWS credentials from Airflow)
def download_readsb_hist_data(day: str):
    """Download 100 files for the given day from S3 to local system."""
    aws_access_key, aws_secret_key = get_aws_credentials()

    # Set up boto3 S3 client with the retrieved credentials
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key,
                             aws_secret_access_key=aws_secret_key)

    bucket_name = 'bdi-aircraft-alexi'  # Your S3 bucket name
    s3_prefix = f"readsb-hist/{day}/"  # S3 prefix (folder structure)

    # Assuming the files are named 'file1.json', 'file2.json', etc. for the day
    for i in range(1, 101):  # Download 100 files
        s3_key = f"{s3_prefix}file{i}.json"
        local_path = f"/path/to/local/files/file{i}.json"  # Change this to your local file path
        try:
            s3_client.download_file(bucket_name, s3_key, local_path)
            print(f"Downloaded {s3_key} to {local_path}")
        except Exception as e:
            print(f"Error downloading {s3_key}: {e}")
            raise

# Function to connect to PostgreSQL database using credentials from Airflow connection
def get_postgresql_credentials():
    """Retrieve PostgreSQL credentials from Airflow connection."""
    conn_id = 'postgres_conn'  # Connection ID for PostgreSQL
    connection = BaseHook.get_connection(conn_id)
    dbname = connection.schema
    user = connection.login
    password = connection.password
    host = connection.host
    port = connection.port
    return dbname, user, password, host, port

# Function to upload data to PostgreSQL
def upload_to_postgresql(day: str):
    """Upload the prepared data to PostgreSQL."""
    # Retrieve PostgreSQL credentials from Airflow connection
    dbname, user, password, host, port = get_postgresql_credentials()

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cursor = conn.cursor()

    try:
        # Implement your PostgreSQL upload logic here
        # For example, inserting data into a table
        query = "INSERT INTO your_table (data) VALUES (%s)"
        with open("/path/to/local/files/processed_data.json") as f:
            data = f.read()
            cursor.execute(query, (data,))
            conn.commit()
            print("Data uploaded to PostgreSQL successfully.")
    except Exception as e:
        print(f"Error uploading to PostgreSQL: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Default arguments for Airflow tasks
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'download_readsb_hist_data',
    default_args=default_args,
    description='Download, prepare, and upload readsb-hist data',
    schedule_interval='@daily',
    start_date=datetime(2023, 11, 1),
    catchup=False,
)

# Define tasks
download_task = PythonOperator(
    task_id='download_readsb_hist_data',
    python_callable=download_readsb_hist_data,
    op_args=['{{ ds }}'],  # Pass the execution date
    dag=dag,
)

upload_postgresql_task = PythonOperator(
    task_id='upload_to_postgresql',
    python_callable=upload_to_postgresql,
    op_args=['{{ ds }}'],
    dag=dag,
)

# Task dependencies
download_task >> upload_postgresql_task
