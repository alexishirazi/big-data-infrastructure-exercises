import gzip
import io
import json

import boto3
import psycopg2
from fastapi import APIRouter
from psycopg2.extras import execute_batch

from bdi_api.settings import DBCredentials, Settings

# Basic setup
settings = Settings()
db_credentials = DBCredentials()
s3_client = boto3.client("s3")
BUCKET_NAME = "bdi-aircraft-alexi"
s7 = APIRouter(prefix="/api/s7", tags=["s7"])

def connect_to_database():
    return psycopg2.connect(
        dbname=db_credentials.database,
        user=db_credentials.username,
        password=db_credentials.password,
        host=db_credentials.host,
        port=db_credentials.port
    )

def create_database_tables():
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aircraft (
            icao VARCHAR PRIMARY KEY,
            registration VARCHAR,
            type VARCHAR
        );
        CREATE TABLE IF NOT EXISTS aircraft_positions (
            icao VARCHAR REFERENCES aircraft(icao),
            timestamp BIGINT,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            altitude_baro DOUBLE PRECISION,
            ground_speed DOUBLE PRECISION,
            emergency BOOLEAN,
            PRIMARY KEY (icao, timestamp)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def get_all_files_from_s3():
    all_data = []
    for obj in s3_client.list_objects_v2(Bucket=BUCKET_NAME).get("Contents", []):
        file_key = obj["Key"]
        file_data = get_file_from_s3(file_key)
        all_data.extend(file_data)
    return all_data

def get_file_from_s3(file_key):
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
    content = obj["Body"].read()

    # First try to decompress with gzip
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            decompressed_content = gz.read()
        data = json.loads(decompressed_content.decode("utf-8"))
    except gzip.BadGzipFile:
        # If it's not a gzipped file, parse as regular JSON
        try:
            data = json.loads(content.decode("utf-8"))
        except UnicodeDecodeError:
            # If standard UTF-8 decoding fails, try with different encodings or error handling
            data = json.loads(content.decode("utf-8", errors="replace"))

    # Handle different JSON structures
    if isinstance(data, dict) and "aircraft" in data:
        return data["aircraft"]
    return data

def save_to_database(data):
    conn = connect_to_database()
    cur = conn.cursor()

    aircraft_data = []
    position_data = []

    for record in data:
        if not isinstance(record, dict):
            continue

        icao = record.get("icao") or record.get("hex")
        if not icao:
            continue

        aircraft_data.append((
            icao,
            record.get("r", ""),
            record.get("type", "")
        ))

        if "lat" in record and "lon" in record:
            position_data.append((
                icao,
                record.get("timestamp", 0),
                record["lat"],
                record["lon"],
                float(record.get("altitude_baro", 0)),
                float(record.get("ground_speed", 0)),
                bool(record.get("emergency", False))
            ))

    if aircraft_data:
        execute_batch(cur, """
            INSERT INTO aircraft (icao, registration, type)
            VALUES (%s, %s, %s)
            ON CONFLICT (icao) DO UPDATE SET
                registration = EXCLUDED.registration,
                type = EXCLUDED.type
        """, aircraft_data)

    if position_data:
        execute_batch(cur, """
            INSERT INTO aircraft_positions
            (icao, timestamp, lat, lon, altitude_baro, ground_speed, emergency)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (icao, timestamp) DO NOTHING
        """, position_data)

    conn.commit()
    cur.close()
    conn.close()

@s7.post("/aircraft/prepare")
def prepare_data():
    create_database_tables()
    data = get_all_files_from_s3()
    if not data:
        return "No aircraft data found"
    save_to_database(data)
    print(f"Saved {len(data)} records to the database")
    return "Aircraft data saved"

@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0):
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(
        "SELECT icao, registration, type FROM aircraft WHERE registration <> '' ORDER BY icao LIMIT %s OFFSET %s",
        (num_results, page * num_results)
    )
    results = [{"icao": r[0], "registration": r[1], "type": r[2]} for r in cur.fetchall()]
    cur.close()
    conn.close()
    return results


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0):
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(
        "SELECT timestamp, lat, lon FROM aircraft_positions WHERE icao = %s ORDER BY timestamp LIMIT %s OFFSET %s",
        (icao, num_results, page * num_results)
    )
    results = [{"timestamp": r[0], "lat": r[1], "lon": r[2]} for r in cur.fetchall()]
    cur.close()
    conn.close()
    return results

@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str):
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(MAX(altitude_baro), 0),
               COALESCE(MAX(ground_speed), 0),
               COALESCE(BOOL_OR(emergency), FALSE)
        FROM aircraft_positions WHERE icao = %s
        """,
        (icao,)
    )
    row = cur.fetchone()
    result = {
        "max_altitude_baro": row[0],
        "max_ground_speed": row[1],
        "had_emergency": row[2]
    }
    cur.close()
    conn.close()
    return result

if __name__ == "__main__":
    print(prepare_data())
