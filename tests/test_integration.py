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

"""End-to-end tests for the ETL pipeline, focusing on the Bronze layer."""
import io
import urllib.parse
import pytest
import psycopg
import csv
import json
import uuid
from datetime import datetime, timezone

from pytest_httpx import HTTPXMock
from testcontainers.postgres import PostgresContainer

from py_load_euctr.config import Settings
from py_load_euctr.extractor import CtisExtractor
from py_load_euctr.loader.postgres import PostgresLoader
from py_load_euctr.models import CtisTrialBronze

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
    "decisionDate": "2023-01-15T00:00:00Z",
}
MOCK_TRIAL_DETAILS_2 = {
    "ctNumber": "2022-500002-02-00",
    "details": "Details for trial 2, with a comma.",
    "decisionDate": "2023-01-16T00:00:00Z",
}


@pytest.fixture(scope="module")
def postgres_container():
    """Starts a PostgreSQL container for the test module."""
    with PostgresContainer(POSTGRES_IMAGE) as container:
        yield container


@pytest.fixture
def db_connection_string(postgres_container: PostgresContainer) -> str:
    """Provides a psycopg-compatible connection string for the test container."""
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    return (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )


@pytest.fixture
def postgres_loader(db_connection_string: str) -> PostgresLoader:
    """Provides a PostgresLoader configured for the test container."""
    return PostgresLoader(db_connection_string)


@pytest.fixture
def mock_settings() -> Settings:
    """Fixture for mock settings."""
    return Settings()


@pytest.mark.asyncio
async def test_full_etl_pipeline(
    postgres_loader: PostgresLoader,
    db_connection_string: str,
    mock_settings: Settings,
    httpx_mock: HTTPXMock,
):
    """
    Tests the full ETL pipeline from extraction to loading into the Bronze
    layer, ensuring data integrity.
    """
    # 1. Setup: Mock API
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json=MOCK_SEARCH_RESPONSE,
    )
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2022-500001-01-00"),
        json=MOCK_TRIAL_DETAILS_1,
    )
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2022-500002-02-00"),
        json=MOCK_TRIAL_DETAILS_2,
    )

    schema_name = "raw"
    table_name = "ctis_trials"
    load_id = str(uuid.uuid4())

    # 2. Extract and Prepare Data (mimicking example.py)
    extractor = CtisExtractor(settings=mock_settings)
    string_buffer = io.StringIO()
    writer = csv.writer(string_buffer, delimiter="\t", quoting=csv.QUOTE_MINIMAL)

    async for trial_data in extractor.extract_trials():
        source_url = extractor.RETRIEVE_URL_TEMPLATE.format(
            ct_number=trial_data["ctNumber"]
        )
        bronze_record = CtisTrialBronze(
            load_id=load_id,
            extracted_at_utc=datetime.now(timezone.utc),
            source_url=source_url,
            data=trial_data,
        )
        model_dict = bronze_record.model_dump()
        model_dict["data"] = json.dumps(model_dict["data"])
        writer.writerow(
            [
                model_dict["load_id"],
                model_dict["extracted_at_utc"].isoformat(),
                model_dict["source_url"],
                model_dict["data"],
            ]
        )

    string_buffer.seek(0)
    bytes_buffer = io.BytesIO(string_buffer.getvalue().encode("utf-8"))

    # 3. Load
    with postgres_loader as loader:
        loader.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        loader.execute_sql(
            f"""
            CREATE TABLE {schema_name}.{table_name} (
                _load_id VARCHAR(36) NOT NULL,
                _extracted_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
                _source_url TEXT,
                data JSONB
            );
        """
        )
        loader.bulk_load_stream(
            target_table=f"{schema_name}.{table_name}",
            data_stream=bytes_buffer,
            columns=["_load_id", "_extracted_at_utc", "_source_url", "data"],
            delimiter="\t",
        )

    # 4. Verify
    with psycopg.connect(db_connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
            assert cur.fetchone()[0] == 2

            cur.execute(
                f"SELECT data FROM {schema_name}.{table_name} WHERE data->>'ctNumber' = %s;",
                ("2022-500002-02-00",),
            )
            loaded_data = cur.fetchone()[0]
            assert loaded_data["ctNumber"] == MOCK_TRIAL_DETAILS_2["ctNumber"]
            assert loaded_data["details"] == MOCK_TRIAL_DETAILS_2["details"]


@pytest.mark.asyncio
async def test_delta_load_pipeline(
    postgres_loader: PostgresLoader,
    db_connection_string: str,
    mock_settings: Settings,
    httpx_mock: HTTPXMock,
):
    """Tests the delta/incremental load functionality."""
    from py_load_euctr.utils import get_last_decision_date

    schema_name = "raw"
    table_name = "ctis_trials_delta"
    load_id_1 = str(uuid.uuid4())
    load_id_2 = str(uuid.uuid4())

    # --- Initial Load ---
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json={
            "pagination": {"page": 1, "size": 1, "totalPages": 1, "nextPage": False},
            "data": [{"ctNumber": "2023-001", "ctTitle": "Initial Trial"}],
        },
    )
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2023-001"),
        json={"ctNumber": "2023-001", "decisionDate": "2023-05-10T00:00:00Z"},
    )

    extractor = CtisExtractor(settings=mock_settings)
    string_buffer_1 = io.StringIO()
    writer_1 = csv.writer(string_buffer_1, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
    async for trial_data in extractor.extract_trials():
        bronze_record = CtisTrialBronze(
            load_id=load_id_1,
            extracted_at_utc=datetime.now(timezone.utc),
            source_url="",
            data=trial_data,
        )
        model_dict = bronze_record.model_dump()
        model_dict["data"] = json.dumps(model_dict["data"])
        writer_1.writerow(
            [
                model_dict["load_id"],
                model_dict["extracted_at_utc"].isoformat(),
                model_dict["source_url"],
                model_dict["data"],
            ]
        )
    string_buffer_1.seek(0)

    with postgres_loader as loader:
        loader.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        loader.execute_sql(
            f"""
            CREATE TABLE {schema_name}.{table_name} (
                _load_id VARCHAR(36), _extracted_at_utc TIMESTAMPTZ,
                _source_url TEXT, data JSONB
            );
        """
        )
        loader.bulk_load_stream(
            target_table=f"{schema_name}.{table_name}",
            data_stream=io.BytesIO(string_buffer_1.getvalue().encode("utf-8")),
            columns=["_load_id", "_extracted_at_utc", "_source_url", "data"],
            delimiter="\t",
        )

    with psycopg.connect(db_connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
            assert cur.fetchone()[0] == 1

    # --- Delta Load ---
    from_date = get_last_decision_date(db_connection_string, schema_name, table_name)
    assert from_date == "2023-05-10"

    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json={
            "pagination": {"page": 1, "size": 1, "totalPages": 1, "nextPage": False},
            "data": [{"ctNumber": "2023-002", "ctTitle": "Delta Trial"}],
        },
    )
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2023-002"),
        json={"ctNumber": "2023-002", "decisionDate": "2023-05-11T00:00:00Z"},
    )

    extractor_delta = CtisExtractor(settings=mock_settings)
    string_buffer_2 = io.StringIO()
    writer_2 = csv.writer(string_buffer_2, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
    async for trial_data in extractor_delta.extract_trials(
        from_decision_date=from_date
    ):
        bronze_record = CtisTrialBronze(
            load_id=load_id_2,
            extracted_at_utc=datetime.now(timezone.utc),
            source_url="",
            data=trial_data,
        )
        model_dict = bronze_record.model_dump()
        model_dict["data"] = json.dumps(model_dict["data"])
        writer_2.writerow(
            [
                model_dict["load_id"],
                model_dict["extracted_at_utc"].isoformat(),
                model_dict["source_url"],
                model_dict["data"],
            ]
        )
    string_buffer_2.seek(0)

    with postgres_loader as loader:
        loader.bulk_load_stream(
            target_table=f"{schema_name}.{table_name}",
            data_stream=io.BytesIO(string_buffer_2.getvalue().encode("utf-8")),
            columns=["_load_id", "_extracted_at_utc", "_source_url", "data"],
            delimiter="\t",
        )

    # --- Final Verification ---
    with psycopg.connect(db_connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
            assert cur.fetchone()[0] == 2
            cur.execute(
                f"SELECT COUNT(*) FROM {schema_name}.{table_name} WHERE data->>'ctNumber' = '2023-002';"
            )
            assert cur.fetchone()[0] == 1


@pytest.mark.asyncio
async def test_api_error_handling(mock_settings: Settings, httpx_mock: HTTPXMock):
    """Tests that the extractor handles API errors gracefully."""
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        status_code=500,
    )
    extractor = CtisExtractor(settings=mock_settings)
    trials = [trial async for trial in extractor.extract_trials()]
    assert len(trials) == 0


@pytest.mark.asyncio
async def test_empty_search_results(
    postgres_loader: PostgresLoader,
    db_connection_string: str,
    mock_settings: Settings,
    httpx_mock: HTTPXMock,
):
    """Tests the pipeline with an empty search result."""
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json={
            "pagination": {"page": 1, "size": 0, "totalPages": 0, "nextPage": False},
            "data": [],
        },
    )
    extractor = CtisExtractor(settings=mock_settings)
    trials = [trial async for trial in extractor.extract_trials()]
    assert len(trials) == 0


@pytest.mark.asyncio
async def test_missing_trial_details(
    postgres_loader: PostgresLoader,
    db_connection_string: str,
    mock_settings: Settings,
    httpx_mock: HTTPXMock,
):
    """Tests that the pipeline handles trials with missing details gracefully."""
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
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2022-500003-03-00"),
        json={"ctNumber": "2022-500003-03-00", "details": "Details for trial 3."},
    )
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2022-500004-04-00"),
        status_code=404,
    )

    schema_name = "raw"
    table_name = "ctis_trials_missing"
    load_id = str(uuid.uuid4())

    extractor = CtisExtractor(settings=mock_settings)
    string_buffer = io.StringIO()
    writer = csv.writer(string_buffer, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
    async for trial_data in extractor.extract_trials():
        source_url = CtisExtractor.RETRIEVE_URL_TEMPLATE.format(
            ct_number=trial_data["ctNumber"]
        )
        bronze_record = CtisTrialBronze(
            load_id=load_id,
            extracted_at_utc=datetime.now(timezone.utc),
            source_url=source_url,
            data=trial_data,
        )
        model_dict = bronze_record.model_dump()
        model_dict["data"] = json.dumps(model_dict["data"])
        writer.writerow(
            [
                model_dict["load_id"],
                model_dict["extracted_at_utc"].isoformat(),
                model_dict["source_url"],
                model_dict["data"],
            ]
        )
    string_buffer.seek(0)
    bytes_buffer = io.BytesIO(string_buffer.getvalue().encode("utf-8"))

    with postgres_loader as loader:
        loader.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        loader.execute_sql(
            f"""
            CREATE TABLE {schema_name}.{table_name} (
                _load_id VARCHAR(36), _extracted_at_utc TIMESTAMPTZ,
                _source_url TEXT, data JSONB
            );
        """
        )
        loader.bulk_load_stream(
            target_table=f"{schema_name}.{table_name}",
            data_stream=bytes_buffer,
            columns=["_load_id", "_extracted_at_utc", "_source_url", "data"],
            delimiter="\t",
        )

    with psycopg.connect(db_connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
            assert cur.fetchone()[0] == 1
            cur.execute(
                f"SELECT data FROM {schema_name}.{table_name} WHERE data->>'ctNumber' = %s;",
                ("2022-500003-03-00",),
            )
            assert cur.fetchone() is not None


@pytest.mark.asyncio
async def test_extractor_pagination(
    postgres_loader: PostgresLoader,
    db_connection_string: str,
    mock_settings: Settings,
    httpx_mock: HTTPXMock,
):
    """Tests that the extractor correctly handles paginated search results."""
    # Page 1
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json={
            "pagination": {"page": 1, "size": 1, "totalPages": 2, "nextPage": True},
            "data": [{"ctNumber": "2023-001", "ctTitle": "Paginated Trial 1"}],
        },
    )
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2023-001"),
        json={"ctNumber": "2023-001", "details": "Details for paginated trial 1."},
    )
    # Page 2
    httpx_mock.add_response(
        method="POST",
        url=CtisExtractor.SEARCH_URL,
        json={
            "pagination": {"page": 2, "size": 1, "totalPages": 2, "nextPage": False},
            "data": [{"ctNumber": "2023-002", "ctTitle": "Paginated Trial 2"}],
        },
    )
    httpx_mock.add_response(
        method="GET",
        url=CtisExtractor.RETRIEVE_URL_TEMPLATE.format(ct_number="2023-002"),
        json={"ctNumber": "2023-002", "details": "Details for paginated trial 2."},
    )

    schema_name = "raw"
    table_name = "ctis_trials_pagination"
    load_id = str(uuid.uuid4())

    extractor = CtisExtractor(settings=mock_settings)
    string_buffer = io.StringIO()
    writer = csv.writer(string_buffer, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
    async for trial_data in extractor.extract_trials():
        source_url = CtisExtractor.RETRIEVE_URL_TEMPLATE.format(
            ct_number=trial_data["ctNumber"]
        )
        bronze_record = CtisTrialBronze(
            load_id=load_id,
            extracted_at_utc=datetime.now(timezone.utc),
            source_url=source_url,
            data=trial_data,
        )
        model_dict = bronze_record.model_dump()
        model_dict["data"] = json.dumps(model_dict["data"])
        writer.writerow(
            [
                model_dict["load_id"],
                model_dict["extracted_at_utc"].isoformat(),
                model_dict["source_url"],
                model_dict["data"],
            ]
        )
    string_buffer.seek(0)
    bytes_buffer = io.BytesIO(string_buffer.getvalue().encode("utf-8"))

    with postgres_loader as loader:
        loader.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        loader.execute_sql(
            f"""
            CREATE TABLE {schema_name}.{table_name} (
                _load_id VARCHAR(36), _extracted_at_utc TIMESTAMPTZ,
                _source_url TEXT, data JSONB
            );
        """
        )
        loader.bulk_load_stream(
            target_table=f"{schema_name}.{table_name}",
            data_stream=bytes_buffer,
            columns=["_load_id", "_extracted_at_utc", "_source_url", "data"],
            delimiter="\t",
        )

    with psycopg.connect(db_connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};")
            assert cur.fetchone()[0] == 2
