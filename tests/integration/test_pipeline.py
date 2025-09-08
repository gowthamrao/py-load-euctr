import pytest
import psycopg
from httpx import Response
from pytest_httpx import HTTPXMock
from testcontainers.postgres import PostgresContainer

from py_load_euctr.cli import LoadMode, arun_pipeline

# Mark all tests in this file as integration tests (R.7.3.3)
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container():
    """Starts a PostgreSQL container once for all tests in this module."""
    with PostgresContainer("postgres:15-alpine") as postgres:
        # The container auto-starts and exposes a random port.
        # Testcontainers manages its lifecycle.
        yield postgres


@pytest.fixture
def db_connection(postgres_container: PostgresContainer):
    """Provides a direct psycopg connection to the test database for assertions."""
    dsn = postgres_container.get_connection_url()
    with psycopg.connect(dsn) as conn:
        yield conn


def mock_euctr_api(httpx_mock: HTTPXMock):
    """Mocks the EUCTR portal API responses for a predictable test."""
    # Mock the search results page (one page only)
    httpx_mock.add_response(
        url__regex=r".*/search.*",  # Simplified regex for robustness
        json={
            "results": [
                {
                    "url": "https://www.clinicaltrialsregister.eu/ctr-search/trial/2020-001234-56/details"
                },
                {
                    "url": "https://www.clinicaltrialsregister.eu/ctr-search/trial/2021-002345-67/details"
                },
            ],
            "has_next_page": False,
        },
    )
    # Mock the details for the first trial
    httpx_mock.add_response(
        url="https://www.clinicaltrialsregister.eu/ctr-search/trial/2020-001234-56/details",
        json={
            "eudract_number": "2020-001234-56",
            "full_title": "A Study of Test Drug A",
            "sponsor_protocol_number": "SP-123",
            "trial_status": "Ongoing",
            "date_of_competent_authority_decision": "2020-01-15",
            "trial_end_date": None,
        },
    )
    # Mock the details for the second trial (updated data)
    httpx_mock.add_response(
        url="https://www.clinicaltrialsregister.eu/ctr-search/trial/2021-002345-67/details",
        json={
            "eudract_number": "2021-002345-67",
            "full_title": "An Investigation of Placebo B",
            "sponsor_protocol_number": "SP-456",
            "trial_status": "Completed",
            "date_of_competent_authority_decision": "2021-03-20",
            "trial_end_date": "2022-12-31",
        },
    )


@pytest.mark.asyncio
async def test_full_pipeline_idempotency(
    postgres_container: PostgresContainer,
    db_connection: psycopg.Connection,
    httpx_mock: HTTPXMock,
):
    """
    Tests the full ELT pipeline end-to-end against a real PostgreSQL instance.
    Verifies data loading and, critically, the idempotency of the transformation (R.7.3.4).
    """
    mock_euctr_api(httpx_mock)
    db_dsn = postgres_container.get_connection_url().replace(
        "psycopg2", "postgresql"
    )  # Testcontainers DSN compatibility

    # --- 1. First run of the pipeline ---
    await arun_pipeline(
        load_mode=LoadMode.FULL,
        db_dsn_override=db_dsn,
        config_file=None,  # Use default settings
    )

    # --- 2. Verify the results of the first run ---
    with db_connection.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Verify Bronze layer (append-only)
        cur.execute("SELECT count(*) FROM raw.euctr_trials;")
        assert cur.fetchone()["count"] == 2

        # Verify Silver layer (transformed)
        cur.execute("SELECT * FROM silver.trials WHERE trial_id = %s;", ("2021-002345-67",))
        silver_rec = cur.fetchone()
        assert silver_rec["title"] == "An Investigation of Placebo B"
        assert silver_rec["status"] == "Completed"

    # --- 3. Second run of the pipeline (to test idempotency) ---
    # We use the same mock data. The Bronze layer will get 2 more records,
    # but the Silver layer should remain unchanged due to the UPSERT logic.
    await arun_pipeline(
        load_mode=LoadMode.FULL,
        db_dsn_override=db_dsn,
        config_file=None,
    )

    # --- 4. Verify idempotency ---
    with db_connection.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # The number of records in Bronze should double (append-only nature)
        cur.execute("SELECT count(*) FROM raw.euctr_trials;")
        assert cur.fetchone()["count"] == 4

        # The number of records in Silver should NOT change (UPSERT logic)
        cur.execute("SELECT count(*) FROM silver.trials;")
        assert cur.fetchone()["count"] == 2

        # Also check that the data is still correct and wasn't mangled
        cur.execute("SELECT status FROM silver.trials WHERE trial_id = %s;", ("2021-002345-67",))
        assert cur.fetchone()["status"] == "Completed"
