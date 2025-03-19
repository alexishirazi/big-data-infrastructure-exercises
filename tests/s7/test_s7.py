import pytest
import json
import gzip
import io
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from bdi_api.s7.exercise import (
    connect_to_database,
    create_database_tables,
    get_file_from_s3,
    get_all_files_from_s3,
    save_to_database,
    prepare_data,
    list_aircraft,
    get_aircraft_position,
    get_aircraft_statistics,
    s7,
)
from fastapi import FastAPI
from psycopg2.extras import execute_batch

# Mock FastAPI app
app = FastAPI()
app.include_router(s7)
client = TestClient(app)

# Fixtures
@pytest.fixture
def mock_db_connection():
    """Mock database connection"""
    conn_mock = MagicMock()
    cursor_mock = MagicMock()
    conn_mock.cursor.return_value = cursor_mock
    cursor_mock.fetchall.return_value = []
    with patch("psycopg2.connect", return_value=conn_mock):
        yield conn_mock, cursor_mock

@pytest.fixture
def mock_s3_client():
    """Mock S3 client"""
    s3_client = MagicMock()
    with patch("boto3.client", return_value=s3_client):
        yield s3_client

@pytest.fixture
def sample_aircraft_data():
    return [
        {
            "icao": "a1b2c3",
            "registration": "N12345",
            "type": "B738",
            "lat": 37.7749,
            "lon": -122.4194,
            "altitude_baro": 10000,
            "ground_speed": 450,
            "emergency": False,
            "file_timestamp": 1645000000,
            "seen": 10,
        }
    ]

# Tests for database connection
def test_connect_to_database(mock_db_connection):
    """Test database connection"""
    conn, _ = mock_db_connection
    result = connect_to_database()
    assert result == conn

def test_connect_to_database_error():
    """Test database connection error handling"""
    with patch("psycopg2.connect", side_effect=Exception("Connection error")):
        with pytest.raises(Exception) as excinfo:
            connect_to_database()
        assert "Connection error" in str(excinfo.value)

# Tests for database table creation
def test_create_database_tables(mock_db_connection):
    """Test database table creation"""
    conn, cursor = mock_db_connection
    create_database_tables()
    assert cursor.execute.called
    assert conn.commit.called

def test_create_database_tables_error(mock_db_connection):
    """Test error handling in table creation"""
    conn, cursor = mock_db_connection
    cursor.execute.side_effect = Exception("SQL error")
    create_database_tables()
    assert conn.rollback.called

# Tests for S3 file retrieval
def test_get_file_from_s3(mock_s3_client):
    """Test retrieving a file from S3"""
    s3_client = mock_s3_client
    sample_data = {"aircraft": [{"icao": "abc123"}]}
    json_bytes = json.dumps(sample_data).encode("utf-8")
    gzipped_content = io.BytesIO()
    with gzip.GzipFile(fileobj=gzipped_content, mode="wb") as f:
        f.write(json_bytes)
    gzipped_bytes = gzipped_content.getvalue()

    s3_client.get_object.return_value = {
        "Body": io.BytesIO(gzipped_bytes),
        "LastModified": MagicMock(timestamp=lambda: 1646000000),
    }

    result = get_file_from_s3("test_file.json.gz", s3_client=s3_client)
    assert len(result) == 1
    assert result[0]["icao"] == "abc123"
    assert result[0]["file_timestamp"] == 1646000000

def test_get_all_files_from_s3(mock_s3_client):
    """Test retrieving all files from S3"""
    s3_client = mock_s3_client
    s3_client.list_objects_v2.return_value = {
        "Contents": [{"Key": "file1.json"}, {"Key": "file2.json"}]
    }

    with patch("bdi_api.s7.exercise.get_file_from_s3", side_effect=[[{"icao": "abc123"}], [{"icao": "def456"}]]):
        result = get_all_files_from_s3(s3_client=s3_client)
    assert len(result) == 2
    assert result[0]["icao"] == "abc123"
    assert result[1]["icao"] == "def456"

# Tests for saving data to database
@patch("bdi_api.s7.exercise.execute_batch")
def test_save_to_database(mock_execute_batch, mock_db_connection, sample_aircraft_data):
    """Test saving data to the database"""
    conn, cursor = mock_db_connection
    save_to_database(sample_aircraft_data)
    
    # Assert that execute_batch was called for aircraft and position data
    assert mock_execute_batch.call_count == 2
    assert conn.commit.called

def test_save_to_database_error(mock_db_connection, sample_aircraft_data):
    """Test error handling when saving to database"""
    conn, cursor = mock_db_connection
    cursor.execute.side_effect = Exception("Database error")
    save_to_database(sample_aircraft_data)
    assert conn.rollback.called

# Tests for API endpoints
def test_prepare_data_endpoint(mock_db_connection, mock_s3_client):
    """Test prepare data endpoint"""
    conn, cursor = mock_db_connection
    s3_client = mock_s3_client
    s3_client.list_objects_v2.return_value = {"Contents": [{"Key": "file1.json"}]}
    with patch("bdi_api.s7.exercise.get_file_from_s3", return_value=[{"icao": "abc123"}]):
        response = client.post("/api/s7/aircraft/prepare")
        assert response.status_code == 200

def test_list_aircraft(mock_db_connection):
    """Test listing aircraft"""
    conn, cursor = mock_db_connection
    cursor.fetchall.return_value = [("abc123", "N12345", "B738")]
    response = list_aircraft()
    assert len(response) == 1
    assert response[0]["icao"] == "abc123"

def test_get_aircraft_position(mock_db_connection):
    """Test getting aircraft positions"""
    conn, cursor = mock_db_connection
    cursor.fetchall.return_value = [(1645000000, 37.7749, -122.4194)]
    response = get_aircraft_position("abc123")
    assert len(response) == 1
    assert response[0]["timestamp"] == 1645000000

def test_get_aircraft_statistics(mock_db_connection):
    """Test getting aircraft statistics"""
    conn, cursor = mock_db_connection
    cursor.fetchone.return_value = (35000, 550, True)
    response = get_aircraft_statistics("abc123")
    assert response["max_altitude_baro"] == 35000
    assert response["max_ground_speed"] == 550
    assert response["had_emergency"] is True