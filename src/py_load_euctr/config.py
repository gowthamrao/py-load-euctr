from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field


class Settings(BaseSettings):
    """
    Manages configuration for the application.

    Reads settings from environment variables with the prefix 'EUCTR_'.
    """

    model_config = SettingsConfigDict(env_prefix="EUCTR_")

    # Database connection settings
    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = "postgres"
    db_password: str = "postgres"
    db_name: str = "euctr"

    @computed_field
    @property
    def db_connection_string(self) -> str:
        """
        Constructs the libpq connection string from individual settings.
        """
        return (
            f"host='{self.db_host}' port='{self.db_port}' "
            f"user='{self.db_user}' password='{self.db_password}' "
            f"dbname='{self.db_name}'"
        )


# Instantiate the settings so it can be imported directly
settings = Settings()
