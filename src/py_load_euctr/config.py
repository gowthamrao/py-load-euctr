# Copyright 2025 Gowtham Rao <rao@ohdsi.org>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Manages the application's configuration using Pydantic."""

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Manages configuration for the application.

    Reads settings from environment variables with the prefix 'EUCTR_'.
    """

    model_config = SettingsConfigDict(env_prefix="EUCTR_")

    # Database connection settings
    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = "postgres"
    # S105: Hardcoded password is used for local development.
    # In production, this should be set via environment variables.
    db_password: str = "postgres"
    db_name: str = "euctr"

    @computed_field
    @property
    def db_connection_string(self) -> str:
        """Construct the libpq connection string from individual settings."""
        return (
            f"host='{self.db_host}' port='{self.db_port}' "
            f"user='{self.db_user}' password='{self.db_password}' "
            f"dbname='{self.db_name}'"
        )


# Instantiate the settings so it can be imported directly
settings = Settings()
