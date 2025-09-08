from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    """
    Manages configuration for the application.
    """

    model_config = SettingsConfigDict(env_prefix="EUROSTAT_")

    # Base URL for Eurostat API
    base_url: str = "https://ec.europa.eu/eurostat/api/dissemination"

    # Path to the local cache directory
    cache_dir: Path = Path.home() / ".eurostat_cache"


# Instantiate the settings so it can be imported directly
settings = Settings()
