import os
from os.path import dirname, join
from dotenv import load_dotenv
from pydantic import Field  # Import Field from pydantic
from pydantic_settings import BaseSettings, SettingsConfigDict  # BaseSettings from pydantic_settings
import bdi_api

# Load environment variables from .env
load_dotenv()

# Get the project directory
PROJECT_DIR = dirname(dirname(bdi_api.__file__))


class DBCredentials(BaseSettings):
    host: str
    username: str
    password: str
    database: str
    port: int = 5432

    model_config = SettingsConfigDict(env_prefix="bdi_db_")


# Instantiating DBCredentials after loading the .env variables
db_credentials = DBCredentials()

# Output for debugging to confirm values are loaded
print(db_credentials)

class Settings(BaseSettings):
    """Main settings for the application"""

    source_url: str = Field(
        default="https://samples.adsbexchange.com/readsb-hist",
        description="Base URL to the website used to download the data."
    )
    local_dir: str = Field(
        default=join(PROJECT_DIR, "data"),
        description="For any other value, set the env variable 'BDI_LOCAL_DIR'"
    )
    s3_bucket: str = Field(
        default="bdi-test",
        description="Call the API like BDI_S3_BUCKET=yourbucket poetry run uvicorn..."
    )
    telemetry: bool = False
    telemetry_dsn: str = "http://project2_secret_token@uptrace:14317/2"

    class Config:
        env_prefix = "BDI_"

    @property
    def raw_dir(self) -> str:
        """Directory for storing raw JSON data"""
        return join(self.local_dir, "raw")

    @property
    def prepared_dir(self) -> str:
        """Directory for storing prepared data"""
        return join(self.local_dir, "prepared")


# Now you can instantiate the `Settings` class after loading the environment variables
settings = Settings()
print(settings)
