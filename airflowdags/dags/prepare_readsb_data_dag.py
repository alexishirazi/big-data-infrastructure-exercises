import json
import logging
import os
from io import BytesIO

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0
}

dag = DAG(
    dag_id='prepare_readsb_data_dag',
    description='Prepare readsb JSON.gz files from S3 RAW and generate Parquet + index in S3 PREPARED, '
                'then upload to Postgres',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        "day": Param("20231101", type="string")
    },
    tags=['prepare', 'readsb']
)

def parse_emergency(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() == "none":
        return False
    if pd.isna(value):
        return False
    return bool(value)

def flush_to_s3(s3, buffer, part_number, prepared_prefix, bucket):
    df = pd.DataFrame(buffer)
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3_key = f"{prepared_prefix}data_part_{part_number}.parquet"
    s3.upload_fileobj(buf, bucket, s3_key)
    return df['icao'].dropna().unique().tolist()

def process_aircraft_data(s3, file_data, record_buffer, parquet_part, index_map, prepared_prefix, bucket):
    for aircraft in file_data.get("aircraft", []):
        if not (aircraft.get("hex") and aircraft.get("lat") and aircraft.get("lon")):
            continue

        record = {
            "icao": aircraft.get("hex"),
            "registration": aircraft.get("r"),
            "type": aircraft.get("t"),
            "latitude": aircraft.get("lat"),
            "longitude": aircraft.get("lon"),
            "alt_baro": None if aircraft.get("alt_baro") == "ground" else aircraft.get("alt_baro"),
            "gs": aircraft.get("gs"),
            "emergency": parse_emergency(aircraft.get("emergency")),
            "timestamp": file_data.get("now"),
        }
        record_buffer.append(record)

        if len(record_buffer) % 10000 == 0:
            df_tmp = pd.DataFrame(record_buffer)
            table_tmp = pa.Table.from_pandas(df_tmp)
            buf_tmp = BytesIO()
            pq.write_table(table_tmp, buf_tmp, compression="snappy")
            size_mb = buf_tmp.tell() / 1024 / 1024
            if size_mb >= 100:
                icaos_in_part = flush_to_s3(s3, record_buffer, parquet_part, prepared_prefix, bucket)
                index_map[f"data_part_{parquet_part}.parquet"] = icaos_in_part
                parquet_part += 1
                record_buffer.clear()

    return record_buffer, parquet_part, index_map

def process_file(s3, file_obj, record_buffer, parquet_part, index_map, prepared_prefix, bucket):
    file_key = file_obj["Key"]
    try:
        file_response = s3.get_object(Bucket=bucket, Key=file_key)
        file_content = file_response["Body"].read().decode("utf-8")
        file_data = json.loads(file_content)

        # Process the aircraft data in the file
        record_buffer, parquet_part, index_map = process_aircraft_data(
            s3, file_data, record_buffer, parquet_part, index_map, prepared_prefix, bucket
        )

    except Exception as e:
        logging.warning(f"Failed to process file {file_key}: {e}")

    return record_buffer, parquet_part, index_map

@task()
def prepare_data(**context):
    day = context['params']['day']
    bucket = os.getenv("AWS_BUCKET", "bdi-aircraft-alexi")
    raw_prefix = f"raw/day={day}/"
    prepared_prefix = f"prepared/day={day}/"

    s3 = boto3.client("s3")
    raw_files = s3.list_objects_v2(Bucket=bucket, Prefix=raw_prefix).get("Contents", [])

    if not raw_files:
        raise Exception("No files found in raw bucket.")

    record_buffer = []
    parquet_part = 0
    index_map = {}

    for file_obj in raw_files:
        record_buffer, parquet_part, index_map = process_file(
            s3, file_obj, record_buffer, parquet_part, index_map, prepared_prefix, bucket
        )

    if record_buffer:
        icaos_in_part = flush_to_s3(s3, record_buffer, parquet_part, prepared_prefix, bucket)
        index_map[f"data_part_{parquet_part}.parquet"] = icaos_in_part

    # Upload the index map to S3
    index_buf = BytesIO(json.dumps(index_map).encode("utf-8"))
    s3.upload_fileobj(index_buf, bucket, f"{prepared_prefix}index.json")

@task()
def upload_to_postgres():
    context = get_current_context()
    day = context['params']['day']
    bucket = os.getenv("AWS_BUCKET", "bdi-aircraft-alexi")
    prefix = f"prepared/day={day}/"

    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        region_name=os.getenv("AWS_REGION")
    )

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    # Step 1: Get existing ICAOs from the 'aircraft' table
    with engine.connect() as connection:
        result = connection.execute("SELECT DISTINCT icao FROM aircraft")
        existing_icaos = {row[0] for row in result}  # Set comprehension instead of generator expression

    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    for obj in objects:
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue

        buffer = BytesIO()
        s3.download_fileobj(bucket, key, buffer)
        buffer.seek(0)
        table = pq.read_table(buffer)
        df = table.to_pandas()

        # Step 2: Filter out rows with unknown ICAO
        df = df[df["icao"].isin(existing_icaos)]

        if not df.empty:
            df.to_sql("aircraft_positions", engine, if_exists="append", index=False)


# Task dependencies
with dag:
    prepare = prepare_data()
    upload = upload_to_postgres()
    prepare >> upload
