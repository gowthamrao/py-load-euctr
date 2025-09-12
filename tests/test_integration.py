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

"""End-to-end tests for the ETL pipeline."""
import io
import urllib.parse
import httpx
import pytest
import psycopg
from pytest_httpx import HTTPXMock
from testcontainers.postgres import PostgresContainer

from py_load_euctr.config import Settings
from py_load_euctr.extractor import CtisExtractor
from py_load_euctr.loader.postgres import PostgresLoader
from py_load_euctr.parser import parse_trial_to_csv

# Using a specific, lightweight image for postgres for deterministic tests.
POSTGRES_IMAGE = "postgres:16-alpine"

# Mock data for the extractor
MOCK_SEARCH_RESPONSE = {
    "pagination": {"page": 1, "size": 2, "totalPages": 1, "nextPage": False},
    "data": [
        {"ctNumber": "2022-500001-01-00", "ctTitle": "Trial 1"},
        {"ctNumber": "2022-500002-02-00", "ctTitle": "Trial 2"},
    ],
}
MOCK_TRIAL_DETAILS_1 = {
    "ctNumber": "2022-500001-01-00",
    "details": "Details for trial 1.",
}
MOCK_TRIAL_DETAILS_2 = {
    "ctNumber": "2022-500002-02-00",
    "details": "Details for trial 2, with a comma.",
}


@pytest.fixture(scope="module")
def postgres_container():
    """Starts a PostgreSQL container for the test module."""
    with PostgresContainer(POSTGRES_IMAGE) as container:
        yield container


@pytest.fixture
def postgres_loader(postgres_container: PostgresContainer) -> PostgresLoader:
    """Provides a PostgresLoader configured for the test container."""
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    conn_string = (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )
    return PostgresLoader(conn_string)


@pytest.fixture
def mock_settings() -> Settings:
    """Fixture for mock settings."""
    return Settings()


@pytest.mark.asyncio
async def test_full_etl_pipeline(
    postgres_loader: PostgresLoader,
    postgres_container: PostgresContainer,
    mock_settings: Settings,
    httpx_mock: HTTPXMock,
):
    """
    Tests the full ETL pipeline from extraction to loading into PostgreSQL.
    """
    # 1. Setup: Mock API and create database table
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json=MOCK_SEARCH_RESPONSE,
    )
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(
            ct_number="2022-500001-01-00"
        ),
        json=MOCK_TRIAL_DETAILS_1,
    )
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(
            ct_number="2022-500002-02-00"
        ),
        json=MOCK_TRIAL_DETAILS_2,
    )

    table_name = "clinical_trials"
    with postgres_loader as loader:
        loader.execute_sql(
            f"CREATE TABLE {table_name} (ct_number TEXT, details TEXT);"
        )

    # 2. Extract and Parse
    extractor = CtisExtractor(settings=mock_settings)
    csv_output = io.StringIO()
    async for trial in extractor.extract_trials():
        csv_row = parse_trial_to_csv(trial)
        if csv_row:
            csv_output.write(csv_row)

    csv_output.seek(0)
    data_stream = io.BytesIO(csv_output.getvalue().encode("utf-8"))

    # 3. Load
    with postgres_loader as loader:
        loader.bulk_load_stream(
            target_table=table_name,
            data_stream=data_stream,
            columns=["ct_number", "details"],
        )

    # 4. Verify
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    conn_string = (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            assert cur.fetchone()[0] == 2

            cur.execute(
                f"SELECT details FROM {table_name} WHERE ct_number = %s;",
                ("2022-500002-02-00",),
            )
            assert cur.fetchone()[0] == "Details for trial 2, with a comma."


@pytest.mark.asyncio
async def test_api_error_handling(
    mock_settings: Settings, httpx_mock: HTTPXMock
):
    """
    Tests that the extractor handles API errors gracefully.
    """
    # Mock a 500 server error for the search API
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        status_code=500,
    )

    extractor = CtisExtractor(settings=mock_settings)

    # Verify that no trials are returned
    trials = [trial async for trial in extractor.extract_trials()]
    assert len(trials) == 0


@pytest.mark.asyncio
async def test_empty_search_results(
    postgres_loader: PostgresLoader,
    postgres_container: PostgresContainer,
    mock_settings: Settings,
    httpx_mock: HTTPXMock,
):
    """
    Tests the pipeline with an empty search result.
    """
    # Mock API response with no trials
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json={
            "pagination": {"page": 1, "size": 0, "totalPages": 0, "nextPage": False},
            "data": [],
        },
    )

    table_name = "clinical_trials_empty"
    with postgres_loader as loader:
        loader.execute_sql(
            f"CREATE TABLE {table_name} (ct_number TEXT, details TEXT);"
        )

    # Run the ETL process
    extractor = CtisExtractor(settings=mock_settings)
    csv_output = io.StringIO()
    async for trial in extractor.extract_trials():
        csv_row = parse_trial_to_csv(trial)
        if csv_row:
            csv_output.write(csv_row)

    csv_output.seek(0)
    data_stream = io.BytesIO(csv_output.getvalue().encode("utf-8"))

    with postgres_loader as loader:
        loader.bulk_load_stream(
            target_table=table_name,
            data_stream=data_stream,
            columns=["ct_number", "details"],
        )

    # Verify that no data was loaded
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    conn_string = (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            assert cur.fetchone()[0] == 0


@pytest.mark.asyncio
async def test_missing_trial_details(
    postgres_loader: PostgresLoader,
    postgres_container: PostgresContainer,
    mock_settings: Settings,
    httpx_mock: HTTPXMock,
):
    """
    Tests that the pipeline handles trials with missing details.
    """
    # Mock search response with two trials
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json={
            "pagination": {"page": 1, "size": 2, "totalPages": 1, "nextPage": False},
            "data": [
                {"ctNumber": "2022-500003-03-00", "ctTitle": "Trial 3"},
                {"ctNumber": "2022-500004-04-00", "ctTitle": "Trial 4 (missing)"},
            ],
        },
    )

    # Mock a successful response for the first trial
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(
            ct_number="2022-500003-03-00"
        ),
        json={"ctNumber": "2022-500003-03-00", "details": "Details for trial 3."},
    )

    # Mock a 404 for the second trial
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(
            ct_number="2022-500004-04-00"
        ),
        status_code=404,
    )

    table_name = "clinical_trials_missing"
    with postgres_loader as loader:
        loader.execute_sql(
            f"CREATE TABLE {table_name} (ct_number TEXT, details TEXT);"
        )

    # Run the ETL process
    extractor = CtisExtractor(settings=mock_settings)
    csv_output = io.StringIO()
    async for trial in extractor.extract_trials():
        csv_row = parse_trial_to_csv(trial)
        if csv_row:
            csv_output.write(csv_row)

    csv_output.seek(0)
    data_stream = io.BytesIO(csv_output.getvalue().encode("utf-8"))

    with postgres_loader as loader:
        loader.bulk_load_stream(
            target_table=table_name,
            data_stream=data_stream,
            columns=["ct_number", "details"],
        )

    # Verify that only the first trial was loaded
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    conn_string = (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            assert cur.fetchone()[0] == 1
            cur.execute(
                f"SELECT ct_number FROM {table_name} WHERE ct_number = %s;",
                ("2022-500003-03-00",),
            )
            assert cur.fetchone() is not None
