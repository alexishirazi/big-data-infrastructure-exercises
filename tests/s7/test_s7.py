# test_s7.py
import pytest
import json
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
from bdi_api.s7.exercise import s7, create_database_tables, get_all_files_from_s3, save_to_database

# Sample test data
SAMPLE_AIRCRAFT_DATA = [
    {
        "icao": "A12345",
        "registration": "N12345",
        "type": "B737",
        "lat": 40.7128,
        "lon": -74.0060,
        "timestamp": 1647532800,
        "altitude_baro": 35000,
        "ground_speed": 450,
        "emergency": False
    },
    {
        "hex": "B67890",
        "r": "N67890",
        "type": "A320",
        "lat": 34.0522,
        "lon": -118.2437,
        "timestamp": 1647532860,
        "altitude_baro": 30000,
        "ground_speed": 420,
        "emergency": True
    }
]

@pytest.fixture
def client():
    return TestClient(s7)

@pytest.fixture
def mock_db_connection():
    with patch('bdi_api.s7.exercise.psycopg2.connect') as mock_connect:
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        # Correctly format the query as bytes with proper parameter substitution
        mock_cursor.mogrify.side_effect = lambda query, vars: (
            query % tuple(v if v is not None else 'NULL' for v in vars)
        ).encode('utf-8')
        yield mock_conn, mock_cursor

@pytest.fixture
def mock_s3_client():
    with patch('bdi_api.s7.exercise.s3_client') as mock_s3:
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "test1.json.gz"}, {"Key": "test2.json"}]
        }
        mock_s3.get_object.return_value = {
            "Body": Mock(read=lambda: json.dumps(SAMPLE_AIRCRAFT_DATA).encode())
        }
        yield mock_s3

def test_create_database_tables(mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    create_database_tables()
    mock_cursor.execute.assert_called_once()
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

def test_get_all_files_from_s3(mock_s3_client):
    result = get_all_files_from_s3()
    assert len(result) == 4  # Two files, each with SAMPLE_AIRCRAFT_DATA (2 records)
    assert result[0]["icao"] == "A12345"
    mock_s3_client.list_objects_v2.assert_called_once_with(Bucket='bdi-aircraft-alexi')
    assert mock_s3_client.get_object.call_count == 2

def test_save_to_database(mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    save_to_database(SAMPLE_AIRCRAFT_DATA)
    
    # Check aircraft data insertion
    assert mock_cursor.execute.call_count >= 1
    assert mock_conn.commit.called
    assert mock_cursor.close.called
    assert mock_conn.close.called

def test_prepare_data_endpoint(client, mock_s3_client, mock_db_connection):
    with patch('bdi_api.s7.exercise.create_database_tables') as mock_create_tables:
        response = client.post("/api/s7/aircraft/prepare")
        assert response.status_code == 200
        assert response.json() == "Aircraft data saved"
        mock_create_tables.assert_called_once()

def test_list_aircraft_endpoint(client, mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.fetchall.return_value = [
        ("A12345", "N12345", "B737"),
        ("B67890", "N67890", "A320")
    ]
    
    response = client.get("/api/s7/aircraft/?num_results=2&page=0")
    assert response.status_code == 200
    assert len(response.json()) == 2
    assert response.json()[0]["icao"] == "A12345"
    assert response.json()[0]["registration"] == "N12345"

def test_get_aircraft_position_endpoint(client, mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.fetchall.return_value = [
        (1647532800, 40.7128, -74.0060),
    ]
    
    response = client.get("/api/s7/aircraft/A12345/positions?num_results=1")
    assert response.status_code == 200
    result = response.json()
    assert len(result) == 1
    assert result[0]["timestamp"] == 1647532800
    assert result[0]["lat"] == 40.7128
    assert result[0]["lon"] == -74.0060

def test_get_aircraft_statistics_endpoint(client, mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.fetchone.return_value = (35000, 450, True)
    
    response = client.get("/api/s7/aircraft/A12345/stats")
    assert response.status_code == 200
    result = response.json()
    assert result["max_altitude_baro"] == 35000
    assert result["max_ground_speed"] == 450
    assert result["had_emergency"] == True

def test_prepare_data_no_data(client, mock_s3_client):
    mock_s3_client.list_objects_v2.return_value = {}
    response = client.post("/api/s7/aircraft/prepare")
    assert response.status_code == 200
    assert response.json() == "No aircraft data found"