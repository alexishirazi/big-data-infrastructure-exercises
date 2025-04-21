import json
from datetime import datetime
from typing import List, Optional

import psycopg2
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from bdi_api.settings import DBCredentials, Settings

settings = Settings()
db_credentials = DBCredentials()

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)

def connect_to_database():
    return psycopg2.connect(
        dbname=db_credentials.database,
        user=db_credentials.username,
        password=db_credentials.password,
        host=db_credentials.host,
        port=db_credentials.port
    )

class AircraftReturn(BaseModel):
    icao: str
    registration: Optional[str]
    type: Optional[str]
    owner: Optional[str]
    manufacturer: Optional[str]
    model: Optional[str]

class AircraftCO2(BaseModel):
    icao: str
    hours_flown: float
    co2: Optional[float]

@s8.get("/aircraft/", response_model=List[AircraftReturn])
def list_aircraft(
    num_results: int = Query(100, ge=1, description="Number of results per page (min 1)"),
    page: int = Query(0, ge=0, description="Page number (min 0)")
) -> List[AircraftReturn]:
    offset = page * num_results
    conn = connect_to_database()
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT DISTINCT aircraft_positions.icao, aircraft_positions.registration, aircraft_positions.type,
                    aircrafts_db.ownop, aircrafts_db.manufacturer, aircrafts_db.model
                FROM aircraft_positions
                LEFT JOIN aircrafts_db ON aircraft_positions.icao = aircrafts_db.icao
                ORDER BY aircraft_positions.icao ASC
                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (num_results, offset))
            rows = cursor.fetchall()
            if not rows:
                return []
            return [
                AircraftReturn(
                    icao=row[0],
                    registration=row[1],
                    type=row[2],
                    owner=row[3],
                    manufacturer=row[4],
                    model=row[5],
                )
                for row in rows
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}") from e
    finally:
        conn.close()

@s8.get("/aircraft/{icao}/co2", response_model=AircraftCO2)
def get_aircraft_co2(
    icao: str,
    day: str = Query(
        ...,
        regex=r"^\d{4}-\d{2}-\d{2}$",
        description="Date in YYYY-MM-DD format"
    )
) -> AircraftCO2:
    try:
        datetime.strptime(day, "%Y-%m-%d")

        # Load fuel consumption data from S3
        import boto3
        s3 = boto3.client('s3')
        s3_key = "raw/fuel_consumption_data/aircraft_type_fuel_consumption_rates.json"
        try:
            obj = s3.get_object(Bucket="bdi-aircraft-alexi", Key=s3_key)
            fuel_consumption_data = json.loads(obj['Body'].read().decode('utf-8'))
        except s3.exceptions.NoSuchKey as err:
            raise HTTPException(status_code=500, detail="Fuel consumption data file not found in S3") from err

        conn = connect_to_database()
        try:
            with conn.cursor() as cursor:
                query = """
                    SELECT aircraft_positions.type, COUNT(*)
                    FROM aircraft_positions
                    WHERE aircraft_positions.icao = UPPER(%s) AND DATE(TO_TIMESTAMP(aircraft_positions.timestamp)) = %s
                    GROUP BY aircraft_positions.type
                """
                cursor.execute(query, (icao.upper(), day))
                result = cursor.fetchone()

                if not result:
                    return AircraftCO2(icao=icao, hours_flown=0, co2=None)

                aircraft_type, rows_for_day = result

                fuel_rate = fuel_consumption_data.get(aircraft_type)
                if not fuel_rate:
                    return AircraftCO2(icao=icao, hours_flown=0, co2=None)

                hours_flown = (rows_for_day * 5) / 3600
                fuel_used_gal_per_hour = fuel_rate["galph"]
                fuel_used_kg = fuel_used_gal_per_hour * hours_flown * 3.04
                co2_tons = (fuel_used_kg * 3.15) / 907.185

                return AircraftCO2(icao=icao, hours_flown=hours_flown, co2=co2_tons)
        finally:
            conn.close()

    except ValueError as err:
        raise HTTPException(status_code=422, detail="Invalid date format. Use YYYY-MM-DD") from err
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}") from e
