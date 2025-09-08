import io
from unittest.mock import MagicMock, patch

import pytest

from py_load_euctr.loaders.postgres import PostgresLoader

pytestmark = pytest.mark.unit


@pytest.fixture
def mock_psycopg():
    """Mocks the entire psycopg library via a patch, isolating loader from the DB."""
    with patch("py_load_euctr.loaders.postgres.psycopg") as mock_psycopg_lib:
        # Mock the context manager protocol for connect()
        mock_conn = MagicMock()
        mock_psycopg_lib.connect.return_value.__enter__.return_value = mock_conn
        yield mock_psycopg_lib, mock_conn


def test_loader_get_conn(mock_psycopg):
    """Tests that get_conn initiates a connection and starts a transaction."""
    _, mock_conn = mock_psycopg
    loader = PostgresLoader(dsn="test_dsn")

    with loader.get_conn() as conn:
        assert conn is mock_conn  # Assert the connection object is the one we mocked

    # Assert that a transaction was started on the connection
    mock_conn.transaction.assert_called_once()


def test_loader_prepare_schema(mock_psycopg):
    """Tests that prepare_schema executes the correct CREATE SCHEMA SQL."""
    _, mock_conn = mock_psycopg
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    loader = PostgresLoader(dsn="test_dsn")
    loader.prepare_schema("my_table", "my_schema", mock_conn)

    # Assert that execute was called with a psycopg.sql.Composed object
    executed_sql = mock_cursor.execute.call_args[0][0]
    # The psycopg.sql.Identifier makes the exact string complex, so we just
    # check for the key parts to confirm the right command was built.
    assert "CREATE SCHEMA IF NOT EXISTS" in str(executed_sql)
    assert "my_schema" in str(executed_sql)


def test_loader_bulk_load_stream(mock_psycopg):
    """Tests that bulk_load_stream uses the cursor's copy method (R.5.3.1)."""
    _, mock_conn = mock_psycopg
    mock_cursor = MagicMock()
    mock_copy_context = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.copy.return_value.__enter__.return_value = mock_copy_context

    loader = PostgresLoader(dsn="test_dsn")
    data_stream = io.BytesIO(b"col1,col2\nval1,val2")

    loader.bulk_load_stream("my_schema.my_table", data_stream, mock_conn)

    # Assert that the COPY command was initiated, which is the key requirement
    mock_cursor.copy.assert_called_once()
    copy_sql = mock_cursor.copy.call_args[0][0]
    assert "COPY" in str(copy_sql)
    assert "my_schema" in str(copy_sql)
    assert "my_table" in str(copy_sql)
    assert "FORMAT CSV" in str(copy_sql)

    # Assert that data from the stream was written to the copy buffer
    mock_copy_context.write.assert_called_once_with(b"col1,col2\nval1,val2")


def test_loader_execute_sql(mock_psycopg):
    """Tests that execute_sql passes the command through to the cursor."""
    _, mock_conn = mock_psycopg
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    loader = PostgresLoader(dsn="test_dsn")
    sql_command = "SELECT 1;"
    loader.execute_sql(sql_command, mock_conn)

    mock_cursor.execute.assert_called_once_with(sql_command)
