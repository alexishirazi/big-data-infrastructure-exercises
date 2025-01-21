import os

import requests
from typing import List
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
from tqdm import tqdm
import pandas as pd

from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings
import pandas as pd

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1", #download postman to help 
    tags=["s1"],
)
#testing how to use the git push function :)

@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"
# TODO Implement download
   # TODO Implement download
    # Create the directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)

     # Create the directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)

    # Clean the download directory
      # Create directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)
    
    # Clean existing files
    for file in os.listdir(download_dir):
        file_path = os.path.join(download_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
    
    try:
        # Get list of files from the URL
        response = requests.get(base_url)
        response.raise_for_status()  # Added error handling for HTTP status
        
        soup = BeautifulSoup(response.text, 'html.parser')
        files = [
            a['href'] for a in soup.find_all('a') 
            if a['href'].endswith('.json.gz')
        ][:file_limit]  # Apply the file limit
        
        downloaded_count = 0
        for file_name in tqdm(files, desc="Downloading files"):
            file_url = urljoin(base_url, file_name)
            response = requests.get(file_url, stream=True)
            
            if response.status_code == 200:
                file_path = os.path.join(download_dir, file_name[:-3])
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                downloaded_count += 1
            else:
                print(f"Failed to download {file_name}")

        
        return f"Downloaded {downloaded_count} files to {download_dir}"
    
    except requests.RequestException as e:
        return f"Error accessing URL: {str(e)}"
    except Exception as e:
        return f"Error during download: {str(e)}"
    
@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """

    ### must clean the data in this section
# TODO
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")

    # Create directory if it doesn't exist
    os.makedirs(prepared_dir, exist_ok=True)

    # Clean existing files
    for file in os.listdir(prepared_dir):  # This line had incorrect indentation
        file_path = os.path.join(prepared_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

    try:
        all_data = []
        for file in os.listdir(raw_dir):
            if file.endswith('.json'):
                file_path = os.path.join(raw_dir, file)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    if "aircraft" in data:  
                        df = pd.DataFrame(data['aircraft'])
                        df['timestamp'] = data['now']  # Add timestamp directly to DataFrame
                        all_data.append(df)
        
        # Combine all data outside the loop
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Select required columns
        combined_df = combined_df[['hex', 'r', 'type', 't', 'lat', 'lon', 'alt_baro', 'gs', 'emergency','timestamp']]
        
        # Rename columns
        combined_df = combined_df.rename(columns={
            'hex': 'icao', 
            'r': 'registration', 
            't': 'type', 
            'alt_baro': 'altitude_baro',
            'gs': 'ground_speed',
            'emergency': 'had_emergency'
        })
        # Drop rows with NaN values in 'icao', 'registration', and 'type'
        combined_df = combined_df.dropna(subset=['icao', 'registration', 'type'])
        # Process emergency flags
        emergency = ['general', 'lifeguard', 'minfuel', 'nordo', 'unlawful', 'downed', 'reserved']
        combined_df['had_emergency'] = combined_df['had_emergency'].apply(lambda x: x in emergency)
        
        # Save to CSV
        combined_df.to_csv(os.path.join(prepared_dir, 'prepared_data.csv'), index=False)

        print(combined_df)
        return f"Data prepared and saved to {prepared_dir}"

    except Exception as e:
        return f"Error during data preparation: {str(e)}"

@s1.get("/aircraft/") # expecting num_results =100, page -1/ aircraft 
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    # TODO
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    file_path = os.path.join(prepared_dir, 'prepared_data.csv')

    if not os.path.exists(file_path):
        return []

    df = pd.read_csv(file_path)
    #i am dropping all duplicate values and sorting by icao to make sure we only see valid information once
    df = df[['icao', 'registration', 'type']].drop_duplicates().sort_values(by='icao')

    start = page * num_results
    end = start + num_results

    result = df.iloc[start:end].to_dict(orient='records')
    print(result)
    return result


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    # TODO implement and return a list with dictionaries with those values.
    

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    file_path = os.path.join(prepared_dir, 'prepared_data.csv')

    if not os.path.exists(file_path):
        return []

    df = pd.read_csv(file_path)
    df = df[df['icao'] == icao].sort_values(by='timestamp')

    start = page * num_results
    end = start + num_results

    result = df.iloc[start:end][['timestamp', 'lat', 'lon']].to_dict(orient='records')
    print(result)
    return result



@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    # TODO Gather and return the correct statistics for the requested aircraft
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    file_path = os.path.join(prepared_dir, 'prepared_data.csv')

    if not os.path.exists(file_path):
        return {}

    df = pd.read_csv(file_path)
    df = df[df['icao'] == icao]

    if df.empty:
        return {}

    # Convert numpy types to Python native types
    max_altitude_baro = float(df['altitude_baro'].max()) if not pd.isna(df['altitude_baro'].max()) else None
    max_ground_speed = float(df['ground_speed'].max()) if not pd.isna(df['ground_speed'].max()) else None
    had_emergency = bool(df['had_emergency'].any())  # Convert numpy.bool_ to Python bool

    print({
        "max_altitude_baro": max_altitude_baro,
        "max_ground_speed": max_ground_speed,
        "had_emergency": had_emergency
    })
    return {
        "max_altitude_baro": max_altitude_baro,
        "max_ground_speed": max_ground_speed,
        "had_emergency": had_emergency
    }