import io
import gzip
import json
import boto3
from fastapi import APIRouter
from bdi_api.settings import DBCredentials, Settings
import psycopg2
from psycopg2.extras import execute_batch
import logging

# Basic setup
logging.basicConfig(level=logging.INFO)
settings = Settings()
BUCKET_NAME = 'bdi-aircraft-alexi'
s3_client = boto3.client("s3")
s7 = APIRouter(prefix="/api/s7", tags=["s7"])


from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Print environment variables to check if they are loaded
print("DB Host:", os.getenv("BDI_DB_HOST"))
print("DB Port:", os.getenv("BDI_DB_PORT"))
print("DB Username:", os.getenv("BDI_DB_USERNAME"))
print("DB Password:", os.getenv("BDI_DB_PASSWORD"))
print("DB Database:", os.getenv("BDI_DB_DATABASE"))


# Database connection function
def connect_to_database():
    db_credentials = DBCredentials()  # Initialize db_credentials
    try:
        conn = psycopg2.connect(
            dbname=db_credentials.database,
            user=db_credentials.username,
            password=db_credentials.password,
            host=db_credentials.host,
            port=db_credentials.port
        )
        logging.info("Database connection established")
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {str(e)}")
        raise

# Create the database tables
def create_database_tables():
    conn = connect_to_database()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aircraft (
                icao VARCHAR PRIMARY KEY,
                registration VARCHAR,
                type VARCHAR
            );
        """)
        
        cur.execute("""
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
        logging.info("Tables created successfully.")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        conn.rollback()  # Rollback if there's an error
    finally:
        cur.close()
        conn.close()

# Retrieve all files from S3
def get_all_files_from_s3():
    all_data = []
    try:
        for obj in s3_client.list_objects_v2(Bucket=BUCKET_NAME).get("Contents", []):
            file_key = obj["Key"]
            file_data = get_file_from_s3(file_key)
            all_data.extend(file_data)
    except Exception as e:
        logging.error(f"Error fetching files from S3: {str(e)}")
    return all_data

# Retrieve a single file from S3
def get_file_from_s3(file_key):
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        content = obj["Body"].read()
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                data = json.loads(gz.read().decode("utf-8"))
        except:
            data = json.loads(content.decode("utf-8"))
        
        return data.get("aircraft", data) if isinstance(data, dict) else data
    except Exception as e:
        logging.error(f"Error reading file from S3 ({file_key}): {str(e)}")
        return []

# Save data to the database
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
            record.get("registration", "") or record.get("r"),
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
    
    try:
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
        logging.info(f"{len(data)} records saved to database.")
    except Exception as e:
        logging.error(f"Error saving data to database: {str(e)}")
        conn.rollback()  # Rollback if there's an error
    finally:
        cur.close()
        conn.close()

# API endpoint to prepare the data and save it to the database
@s7.post("/aircraft/prepare")
def prepare_data():
    try:
        create_database_tables()
        data = get_all_files_from_s3()
        
        if not data:
            return {"message": "No aircraft data found"}
        
        save_to_database(data)
        return {"message": f"{len(data)} aircraft records saved"}
    
    except Exception as e:
        logging.error(f"Error preparing data: {str(e)}")
        return {"error": f"Internal server error: {str(e)}"}

# List aircraft data
@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0):
    conn = connect_to_database()
    cur = conn.cursor()
    
    try:
        cur.execute(
            "SELECT icao, registration, type FROM aircraft ORDER BY icao LIMIT %s OFFSET %s",
            (num_results, page * num_results)
        )
        results = [{"icao": r[0], "registration": r[1], "type": r[2]} for r in cur.fetchall()]
    except Exception as e:
        logging.error(f"Error retrieving aircraft list: {str(e)}")
        results = []
    finally:
        cur.close()
        conn.close()
    
    return results

# Get positions of a specific aircraft
@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0):
    conn = connect_to_database()
    cur = conn.cursor()
    
    try:
        cur.execute(
            "SELECT timestamp, lat, lon FROM aircraft_positions WHERE icao = %s ORDER BY timestamp LIMIT %s OFFSET %s",
            (icao, num_results, page * num_results)
        )
        results = [{"timestamp": r[0], "lat": r[1], "lon": r[2]} for r in cur.fetchall()]
    except Exception as e:
        logging.error(f"Error retrieving positions for aircraft {icao}: {str(e)}")
        results = []
    finally:
        cur.close()
        conn.close()
    
    return results

# Get statistics for a specific aircraft
@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str):
    conn = connect_to_database()
    cur = conn.cursor()
    
    try:
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
    except Exception as e:
        logging.error(f"Error retrieving statistics for aircraft {icao}: {str(e)}")
        result = {}
    finally:
        cur.close()
        conn.close()
    
    return result

# Start the application
if __name__ == "__main__":
    logging.info("Preparing data...")
    print(prepare_data())
