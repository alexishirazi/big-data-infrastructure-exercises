import json
import os
from unittest import mock

import boto3
import pytest
from moto import mock_s3

from bdi_api.s4.exercise import download_file, process_s3_file, clean_folder


@pytest.fixture
def s3_bucket():
    """Create a mock S3 bucket for testing."""
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bdi-aircraft-bucket"
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    test_dir = "test_output"
    os.makedirs(test_dir, exist_ok=True)
    yield test_dir
    # Clean up if directory exists
    if os.path.exists(test_dir):
        for file in os.listdir(test_dir):
            os.remove(os.path.join(test_dir, file))
        os.rmdir(test_dir)


def test_download_file():
    """Test download_file function."""
    # Setup
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)
        
        # Mock requests.get
        with mock.patch("requests.get") as mock_get:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.content = b"test content"
            mock_get.return_value = mock_response
            
            # Mock the s3_client in the function
            with mock.patch("bdi_api.s4.exercise.s3_client", s3):
                # Test successful download
                result = download_file(
                    "http://example.com/", 
                    "test.json.gz", 
                    bucket_name, 
                    "raw/day=20231101/"
                )
                
                # Verify success
                assert result is True
                
                # Test failed download
                mock_get.side_effect = Exception("Test error")
                result = download_file(
                    "http://example.com/", 
                    "test.json.gz", 
                    bucket_name, 
                    "raw/day=20231101/"
                )
                assert result is False


def test_process_s3_file(temp_dir):
    """Test process_s3_file function."""
    # Setup
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)
        
        # Create and upload test data
        test_data = {
            "now": 1635724800,
            "aircraft": [
                {
                    "hex": "abc123",
                    "r": "N123AB",
                    "t": "B738",
                    "lat": 40.7128,
                    "lon": -74.0060,
                    "alt_baro": 30000,
                    "gs": 450,
                    "alert": 0
                }
            ]
        }
        
        s3.put_object(
            Bucket=bucket_name,
            Key="raw/day=20231101/test.json.gz",
            Body=json.dumps(test_data).encode()
        )
        
        # Mock the s3_client
        with mock.patch("bdi_api.s4.exercise.s3_client", s3):
            # Test successful processing
            result = process_s3_file(bucket_name, "raw/day=20231101/test.json.gz", temp_dir)
            assert result is True
            
            # Verify file was created
            processed_file = os.path.join(temp_dir, "test.json")
            assert os.path.exists(processed_file)
            
            # Test failed processing
            result = process_s3_file(bucket_name, "raw/day=20231101/nonexistent.json.gz", temp_dir)
            assert result is False


def test_clean_folder():
    """Test clean_folder function."""
    # Create a test directory with a file
    test_dir = "test_folder"
    os.makedirs(test_dir, exist_ok=True)
    test_file = os.path.join(test_dir, "test.txt")
    with open(test_file, "w") as f:
        f.write("test")
    
    # Test the function
    clean_folder(test_dir)
    
    # Verify folder exists but is empty
    assert os.path.exists(test_dir)
    assert len(os.listdir(test_dir)) == 0
    
    # Clean up
    os.rmdir(test_dir)


def test_download_data_endpoint():
    """Test the download_data API function directly."""
    from bdi_api.s4.exercise import download_data
    
    # Mock settings
    with mock.patch("bdi_api.s4.exercise.settings.s3_bucket", "test-bucket"):
        with mock.patch("bdi_api.s4.exercise.settings.source_url", "http://example.com"):
            # Mock requests.get
            html_content = '<html><a href="file1.json.gz">file1.json.gz</a></html>'
            mock_response = mock.Mock()
            mock_response.text = html_content
            mock_response.status_code = 200
            mock_response.raise_for_status = mock.Mock()
            
            with mock.patch("bdi_api.s4.exercise.requests.get", return_value=mock_response):
                with mock.patch("bdi_api.s4.exercise.download_file", return_value=True):
                    # Test function directly (not through API)
                    result = download_data(file_limit=1)
                    assert "Downloaded" in result


def test_prepare_data_endpoint():
    """Test the prepare_data API function directly."""
    from bdi_api.s4.exercise import prepare_data
    
    # Create a temp directory for testing
    test_dir = "test_prepared_dir"
    os.makedirs(test_dir, exist_ok=True)
    
    try:
        # Mock settings.s3_bucket and use a real directory
        with mock.patch("bdi_api.s4.exercise.settings.s3_bucket", "test-bucket"):
            with mock.patch("bdi_api.s4.exercise.clean_folder") as mock_clean:
                # Mock the S3 paginator
                mock_paginator = mock.Mock()
                mock_paginator.paginate.return_value = [{
                    "Contents": [{"Key": "raw/day=20231101/file1.json.gz"}]
                }]
                
                with mock.patch("bdi_api.s4.exercise.s3_client.get_paginator", return_value=mock_paginator):
                    with mock.patch("bdi_api.s4.exercise.process_s3_file", return_value=True):
                        # Test function directly
                        result = prepare_data()
                        assert "Processed" in result
    finally:
        # Clean up test directory
        if os.path.exists(test_dir):
            os.rmdir(test_dir)
