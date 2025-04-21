import gzip
import io
import json
from datetime import datetime, timedelta

import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'readsb_hist_download_prepare',
    default_args=default_args,
    description='Download and prepare readsb-hist data for multiple days',
    schedule_interval='@monthly',  # Runs monthly
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2024, 11, 1),
    catchup=True,  # Backfill for past dates
) as dag:

    def download_files_to_s3(**kwargs):
        execution_date = kwargs['execution_date']
        day_str = execution_date.strftime('%Y%m01')  # e.g., 20231101
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_client = boto3.client('s3')
        bucket_name = 'bdi-aircraft-alexi'
        base_url = 'https://data.opensky-network.org/readsb-hist'
        files_downloaded = 0

        for hour in range(24):  # Loop through hours (00 to 23)
            for minute in range(0, 60, 10):  # Loop through minutes (00, 10, 20, ..., 50)
                if files_downloaded >= 100:  # Limit to 100 files
                    break
                file_name = f'{day_str}-{hour:02d}{minute:02d}Z.json.gz'
                url = f'{base_url}/{day_str[:4]}/{day_str[4:6]}/{file_name}'  # e.g., 2023/11/20231101-0000Z.json.gz
                s3_key = f'raw/day={day_str}/{file_name}'

                # Check if file already exists in S3 (idempotency)
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                    continue  # Skip if file exists
                except s3_client.exceptions.ClientError:  # Specify the exception type
                    pass

                # Download the file
                try:
                    response = requests.get(url, stream=True)
                    if response.status_code == 200:
                        s3_hook.load_bytes(
                            response.content,
                            key=s3_key,
                            bucket_name=bucket_name,
                            replace=True
                        )
                        files_downloaded += 1
                    else:
                        continue  # Skip if file not found
                except requests.exceptions.RequestException as e:
                    print(f"Error downloading {url}: {str(e)}")
                    continue

        print(f"Downloaded {files_downloaded} files for {day_str}")

    def process_aircraft_data(aircraft_records):
        aircraft_data = []
        for record in aircraft_records:
            icao = record.get('icao') or record.get('hex')
            if not icao:
                continue
            aircraft_data.append((
                icao,
                record.get('registration', '') or record.get('r'),
                record.get('type', '')
            ))
        return aircraft_data

    def process_position_data(aircraft_records):
        position_data = []
        for record in aircraft_records:
            if 'lat' in record and 'lon' in record:
                timestamp = record.get('timestamp') or record.get('seen')
                if not timestamp:
                    continue
                position_data.append((
                    record['icao'],
                    timestamp,
                    record['lat'],
                    record['lon'],
                    float(record.get('altitude_baro', 0)),
                    float(record.get('ground_speed', 0)),
                    bool(record.get('emergency', False))
                ))
        return position_data

    def prepare_and_load_to_rds(**kwargs):
        execution_date = kwargs['execution_date']
        day_str = execution_date.strftime('%Y%m01')
        s3_hook = S3Hook(aws_conn_id='aws_default')
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        bucket_name = 'bdi-aircraft-alexi'
        prefix = f'raw/day={day_str}/'

        # List files in S3
        files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        if not files:
            print(f"No files found for {day_str}")
            return

        aircraft_data = []
        position_data = []

        for file_key in files[:100]:  # Limit to 100 files
            # Download and process the file
            content = s3_hook.read_key(key=file_key, bucket_name=bucket_name)
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                    data = json.loads(gz.read().decode('utf-8'))
            except gzip.BadGzipFile:
                data = json.loads(content.decode('utf-8'))

            aircraft_records = data.get('aircraft', data) if isinstance(data, dict) else data
            aircraft_data.extend(process_aircraft_data(aircraft_records))
            position_data.extend(process_position_data(aircraft_records))

            # Store prepared data in S3
            prepared_key = file_key.replace('raw/', 'prepared/')
            prepared_data = json.dumps(aircraft_records).encode('utf-8')
            s3_hook.load_bytes(prepared_data, key=prepared_key, bucket_name=bucket_name, replace=True)

        # Load to RDS
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        try:
            if aircraft_data:
                execute_batch(cur, """
                    INSERT INTO aircraft (icao, registration, type)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (icao) DO UPDATE SET
                        registration = EXCLUDED.registration,
                        type = EXCLUDED.type
                """, list(set(aircraft_data)))  # Deduplicate aircraft_data

            if position_data:
                execute_batch(cur, """
                    INSERT INTO aircraft_positions
                    (icao, timestamp, lat, lon, altitude_baro, ground_speed, emergency)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (icao, timestamp) DO NOTHING
                """, position_data)

            conn.commit()
            print(f"Loaded {len(aircraft_data)} aircraft and {len(position_data)} positions for {day_str}")
        except Exception as e:
            print(f"Error loading to RDS: {str(e)}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    # Define tasks
    download_task = PythonOperator(
        task_id='download_files_to_s3',
        python_callable=download_files_to_s3,
        provide_context=True,
    )

    prepare_task = PythonOperator(
        task_id='prepare_and_load_to_rds',
        python_callable=prepare_and_load_to_rds,
        provide_context=True,
    )

    # Set task dependencies
    download_task >> prepare_task
