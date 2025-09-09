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

import os
from py_load_euctr.config import Settings


def test_settings_default_values():
    """
    Tests that the Settings model initializes with correct default values.
    """
    settings = Settings()
    assert settings.db_host == "localhost"
    assert settings.db_port == 5432
    assert settings.db_user == "postgres"
    assert settings.db_password == "postgres"
    assert settings.db_name == "euctr"


def test_settings_from_environment_variables(monkeypatch):
    """
    Tests that the Settings model correctly loads configuration
    from environment variables.
    """
    monkeypatch.setenv("EUCTR_DB_HOST", "testhost")
    monkeypatch.setenv("EUCTR_DB_PORT", "5433")
    monkeypatch.setenv("EUCTR_DB_USER", "testuser")
    monkeypatch.setenv("EUCTR_DB_PASSWORD", "testpass")
    monkeypatch.setenv("EUCTR_DB_NAME", "testdb")

    settings = Settings()

    assert settings.db_host == "testhost"
    assert settings.db_port == 5433
    assert settings.db_user == "testuser"
    assert settings.db_password == "testpass"
    assert settings.db_name == "testdb"


def test_db_connection_string_computed_field():
    """
    Tests that the db_connection_string computed field is generated correctly.
    """
    settings = Settings(
        db_host="db_host",
        db_port=1234,
        db_user="db_user",
        db_password="db_password",
        db_name="db_name",
    )
    expected_conn_str = (
        "host='db_host' port='1234' "
        "user='db_user' password='db_password' "
        "dbname='db_name'"
    )
    assert settings.db_connection_string == expected_conn_str
