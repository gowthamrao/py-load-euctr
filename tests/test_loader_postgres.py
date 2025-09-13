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

import csv
import io
import urllib.parse

import pytest
from testcontainers.postgres import PostgresContainer
import psycopg

from py_load_euctr.loader.postgres import PostgresLoader

# Using a specific, lightweight image for postgres for deterministic tests.
POSTGRES_IMAGE = "postgres:16-alpine"


@pytest.fixture(scope="module")
def postgres_container():
    """
    A pytest fixture that starts a PostgreSQL container for the test module.
    The container will be available for all tests in this file and will be
    torn down after the last test is complete.
    """
    with PostgresContainer(POSTGRES_IMAGE) as container:
        yield container


@pytest.fixture
def postgres_loader(postgres_container: PostgresContainer) -> PostgresLoader:
    """
    A pytest fixture that provides an instance of PostgresLoader
    configured to connect to the running test container.
    """
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    conn_string = (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )
    return PostgresLoader(conn_string)


def test_postgres_loader_connection_and_execution(
    postgres_loader: PostgresLoader, postgres_container: PostgresContainer
):
    """
    Integration test to verify that the PostgresLoader can connect,
    execute SQL, and commit a transaction correctly.
    """
    test_table_name = "test_commit_table"
    create_table_sql = (
        f"CREATE TABLE {test_table_name} (id INT PRIMARY KEY, name VARCHAR(50));"
    )
    insert_sql = f"INSERT INTO {test_table_name} (id, name) VALUES (1, 'test_name');"

    # Use the loader as a context manager to create a table and insert data.
    # The __exit__ method should commit this transaction.
    with postgres_loader as loader:
        loader.execute_sql(create_table_sql)
        loader.execute_sql(insert_sql)

    # To verify the commit, connect to the database again in a new session
    # and check if the data is present.
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    conn_string = (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, name FROM {test_table_name} WHERE id = 1;")
            result = cur.fetchone()
            assert (
                result is not None
            ), "Data should have been committed and be selectable."
            assert result[0] == 1
            assert result[1] == "test_name"


def test_postgres_loader_rollback_on_exception(
    postgres_loader: PostgresLoader, postgres_container: PostgresContainer
):
    """
    Integration test to verify that the PostgresLoader rolls back the
    transaction when an exception occurs within the context block.
    """
    test_table_name = "test_rollback_table"
    create_table_sql = f"CREATE TABLE {test_table_name} (id INT PRIMARY KEY);"

    # Use the loader and intentionally raise an exception to trigger a rollback.
    try:
        with postgres_loader as loader:
            loader.execute_sql(create_table_sql)
            raise ValueError("Intentional failure to trigger rollback")
    except ValueError:
        # We expect this exception.
        pass

    # Connect again to verify that the table creation was rolled back.
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    conn_string = (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            # A reliable way to check for table existence in PostgreSQL.
            cur.execute(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = %s);",
                (test_table_name,),
            )
            table_exists = cur.fetchone()[0]
            assert (
                not table_exists
            ), "Table should not exist after transaction was rolled back."


def test_postgres_loader_bulk_load(
    postgres_loader: PostgresLoader, postgres_container: PostgresContainer
):
    """
    Integration test to verify that the bulk_load_stream method correctly
    loads data into the database using the COPY command.
    """
    test_table_name = "test_bulk_load_table"
    create_table_sql = f"CREATE TABLE {test_table_name} (id INT, name VARCHAR(100));"

    # Prepare sample CSV data in an in-memory bytes buffer.
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([1, "first_row"])
    writer.writerow([2, "second_row"])
    writer.writerow([3, "third_row with comma,"])
    output.seek(0)
    data_stream = io.BytesIO(output.getvalue().encode("utf-8"))

    with postgres_loader as loader:
        loader.execute_sql(create_table_sql)
        loader.bulk_load_stream(
            target_table=test_table_name,
            data_stream=data_stream,
            columns=["id", "name"],
            delimiter=",",
        )

    # Verify the data was loaded correctly.
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    conn_string = (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {test_table_name};")
            count = cur.fetchone()[0]
            assert count == 3

            cur.execute(f"SELECT name FROM {test_table_name} WHERE id = 3;")
            name = cur.fetchone()[0]
            assert name == "third_row with comma,"


def test_postgres_loader_exit_without_enter(postgres_loader: PostgresLoader):
    """
    Tests that calling __exit__ without __enter__ does nothing and
    does not raise an error.
    """
    # This should not raise any exception
    postgres_loader.__exit__(None, None, None)


def test_postgres_loader_raises_runtime_error_if_not_in_context(
    postgres_loader: PostgresLoader,
):
    """
    Tests that calling execute_sql or bulk_load_stream outside of a context
    manager block raises a RuntimeError.
    """
    with pytest.raises(RuntimeError, match="Cursor is not available"):
        postgres_loader.execute_sql("SELECT 1;")

    with pytest.raises(RuntimeError, match="Cursor is not available"):
        postgres_loader.bulk_load_stream("any_table", io.BytesIO(b"any_data"))


def test_postgres_loader_bulk_load_no_columns(
    postgres_loader: PostgresLoader, postgres_container: PostgresContainer
):
    """
    Tests the bulk_load_stream method without specifying columns,
    covering the `else` branch in the method.
    """
    test_table_name = "test_bulk_load_no_cols"
    create_table_sql = f"CREATE TABLE {test_table_name} (id INT, name VARCHAR(100));"

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([10, "no_columns_specified"])
    output.seek(0)
    data_stream = io.BytesIO(output.getvalue().encode("utf-8"))

    with postgres_loader as loader:
        loader.execute_sql(create_table_sql)
        # Call bulk_load_stream without the 'columns' argument
        loader.bulk_load_stream(
            target_table=test_table_name, data_stream=data_stream, delimiter=","
        )

    # Verify the data was loaded correctly.
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    conn_string = (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT name FROM {test_table_name} WHERE id = 10;")
            name = cur.fetchone()[0]
            assert name == "no_columns_specified"


def test_postgres_loader_bulk_load_rollback_on_failure(
    postgres_loader: PostgresLoader, postgres_container: PostgresContainer
):
    """
    Tests that if bulk_load_stream fails, the transaction is rolled back
    and no partial data is committed.
    """
    test_table_name = "test_bulk_rollback"
    create_table_sql = f"CREATE TABLE {test_table_name} (id INT, name VARCHAR(50));"
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    conn_string = (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )

    # Setup: create the table and insert one row to check against later.
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            cur.execute(f"INSERT INTO {test_table_name} VALUES (0, 'initial_row');")
        conn.commit()

    # Create a malformed data stream (3 columns instead of 2)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([1, "good_row"])
    writer.writerow([2, "bad_row", "extra_col"])  # This row will cause an error
    output.seek(0)
    data_stream = io.BytesIO(output.getvalue().encode("utf-8"))

    # Action: Attempt the bulk load, which is expected to fail.
    # The psycopg.errors.BadCopyFileFormat is a good candidate for this error.
    with pytest.raises(psycopg.Error):
        with postgres_loader as loader:
            loader.bulk_load_stream(
                target_table=test_table_name,
                data_stream=data_stream,
                columns=["id", "name"],
                delimiter=",",
            )

    # Verification: Connect again and ensure no new rows were added.
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {test_table_name};")
            count = cur.fetchone()[0]
            # Only the initial row should exist.
            assert (
                count == 1
            ), "Transaction should have been rolled back, leaving no new rows."


def test_postgres_loader_bulk_load_with_schema(
    postgres_loader: PostgresLoader, postgres_container: PostgresContainer
):
    """
    Tests bulk loading into a table with a specific schema.
    """
    schema_name = "my_schema"
    table_name = "test_table"
    qualified_table_name = f"{schema_name}.{table_name}"

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([1, "schema_test"])
    output.seek(0)
    data_stream = io.BytesIO(output.getvalue().encode("utf-8"))

    with postgres_loader as loader:
        loader.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        loader.execute_sql(
            f"CREATE TABLE {qualified_table_name} (id INT, name VARCHAR(100));"
        )
        loader.bulk_load_stream(
            target_table=qualified_table_name,
            data_stream=data_stream,
            columns=["id", "name"],
            delimiter=",",
        )

    # Verify the data was loaded correctly.
    conn_url = postgres_container.get_connection_url()
    parsed = urllib.parse.urlparse(conn_url)
    conn_string = (
        f"host='{parsed.hostname}' port='{parsed.port}' "
        f"user='{parsed.username}' password='{parsed.password}' "
        f"dbname='{parsed.path.lstrip('/')}'"
    )
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT name FROM {qualified_table_name} WHERE id = 1;")
            name = cur.fetchone()[0]
            assert name == "schema_test"


def test_postgres_loader_execute_sql_fetch(
    postgres_loader: PostgresLoader,
):
    """
    Tests the fetch options of the execute_sql method.
    """
    table_name = "test_fetch"
    with postgres_loader as loader:
        loader.execute_sql(f"CREATE TABLE {table_name} (id INT, name VARCHAR(50));")
        loader.execute_sql(f"INSERT INTO {table_name} VALUES (1, 'one'), (2, 'two');")

        # Test fetch="one"
        result_one = loader.execute_sql(
            f"SELECT id, name FROM {table_name} WHERE id = 1;", fetch="one"
        )
        assert result_one == (1, "one")

        # Test fetch="all"
        result_all = loader.execute_sql(
            f"SELECT id, name FROM {table_name} ORDER BY id;", fetch="all"
        )
        assert result_all == [(1, "one"), (2, "two")]

        # Test fetch=None
        result_none = loader.execute_sql("SELECT 1;", fetch=None)
        assert result_none is None
