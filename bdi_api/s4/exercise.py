import concurrent.futures
import gzip
import io
import json
import os
import shutil
from typing import Annotated
from urllib.parse import urljoin

import boto3
import requests
from botocore.config import Config
from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException, status
from fastapi.params import Query
from tqdm import tqdm

from bdi_api.settings import Settings

settings = Settings()
s3_client = boto3.client(
    "s3", config=Config(connect_timeout=5, read_timeout=5, retries={"max_attempts": 2})
)


def clean_folder(folder_path: str) -> None:
    """Clean and recreate a folder."""
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
    os.makedirs(folder_path, exist_ok=True)


s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


def download_file(
    base_url: str, file_name: str, s3_bucket: str, s3_prefix_path: str
) -> bool:
    """Download a file from URL and store it in S3."""
    try:
        file_url = urljoin(base_url, file_name)
        response = requests.get(file_url, stream=True, timeout=10)
        if response.status_code == 200:
            s3_key = f"{s3_prefix_path}{file_name}"
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=response.content)
            return True
        return False
    except Exception:
        return False


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int, Query(..., description="Limit number of files to download")
    ] = 1000
) -> str:
    """Download aircraft data files and store them in S3."""
    if not settings.s3_bucket:
        raise HTTPException(
            status_code=500,
            detail="S3 bucket not configured. Set BDI_S3_BUCKET environment variable.",
        )

    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    try:
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        files = [
            a["href"] for a in soup.find_all("a") if a["href"].endswith(".json.gz")
        ][:file_limit]

        if not files:
            return "No .json.gz files found at the source URL."

        print(f"Found {len(files)} files to download")

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_file = {
                executor.submit(
                    download_file, base_url, file_name, s3_bucket, s3_prefix_path
                ): file_name
                for file_name in files
            }
            successful_downloads = sum(
                1
                for future in tqdm(
                    concurrent.futures.as_completed(future_to_file),
                    total=len(files),
                    desc="Downloading files",
                )
                if future.result()
            )
        return f"Downloaded {successful_downloads} out of {len(files)} files to S3 bucket {s3_bucket}"
    except requests.RequestException as e:
        return f"Error accessing URL: {str(e)}"
    except Exception as e:
        return f"Error during download: {str(e)}"


def process_s3_file(s3_bucket: str, s3_key: str, local_prepared_path: str) -> bool:
    """Process a single S3 file and save it in simplified JSON format."""
    try:
        filename = os.path.basename(s3_key)
        prepared_file_path = os.path.join(
            local_prepared_path, filename.replace(".json.gz", ".json")
        )
        
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        file_content = response["Body"].read()

        try:
            with gzip.GzipFile(fileobj=io.BytesIO(file_content)) as gz_file:
                data = json.loads(gz_file.read().decode("utf-8"))
        except Exception:
            # Try without gzip decompression in case it's already decompressed
            data = json.loads(file_content.decode("utf-8"))

        processed_data = [
            {
                "icao": record.get("hex"),
                "registration": record.get("r"),
                "type": record.get("t"),
                "lat": record.get("lat"),
                "lon": record.get("lon"),
                "alt_baro": record.get("alt_baro"),
                "timestamp": data.get("now"),
                "max_altitude_baro": record.get("alt_baro"),
                "max_ground_speed": record.get("gs"),
                "had_emergency": record.get("alert", 0) == 1,
            }
            for record in data.get("aircraft", [])
        ]

        with open(prepared_file_path, "w", encoding="utf-8") as prepared_file:
            json.dump(processed_data, prepared_file)

        return True
    except Exception as e:
        print(f"Error processing file {s3_key}: {str(e)}")
        return False


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Process aircraft data from S3 and save in prepared format."""
    if not settings.s3_bucket:
        raise HTTPException(
            status_code=500,
            detail="S3 bucket not configured. Set BDI_S3_BUCKET environment variable.",
        )

    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    local_prepared_path = settings.prepared_dir
    
    # Ensure the prepared directory exists
    clean_folder(local_prepared_path)

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix_path)
        all_objects = [
            obj
            for page in page_iterator
            if "Contents" in page
            for obj in page["Contents"]
        ]

        if not all_objects:
            return "No files found in S3."

        print(f"Found {len(all_objects)} files in S3 to process")

        # Process files in batches of 50
        successful_processes = 0
        for batch in tqdm([all_objects[i : i + 50] for i in range(0, len(all_objects), 50)], desc="Processing batches"):
            for obj in batch:
                if process_s3_file(s3_bucket, obj["Key"], local_prepared_path):
                    successful_processes += 1

        return f"Processed {successful_processes} out of {len(all_objects)} files. Data saved to {local_prepared_path}"
    except Exception as e:
        return f"Error processing data: {str(e)}"
