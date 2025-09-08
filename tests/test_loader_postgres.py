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
    conn_string = postgres_container.get_connection_url()
    return PostgresLoader(conn_string)


def test_postgres_loader_connection_and_execution(postgres_loader: PostgresLoader, postgres_container: PostgresContainer):
    """
    Integration test to verify that the PostgresLoader can connect,
    execute SQL, and commit a transaction correctly.
    """
    test_table_name = "test_commit_table"
    create_table_sql = f"CREATE TABLE {test_table_name} (id INT PRIMARY KEY, name VARCHAR(50));"
    insert_sql = f"INSERT INTO {test_table_name} (id, name) VALUES (1, 'test_name');"

    # Use the loader as a context manager to create a table and insert data.
    # The __exit__ method should commit this transaction.
    with postgres_loader as loader:
        loader.execute_sql(create_table_sql)
        loader.execute_sql(insert_sql)

    # To verify the commit, connect to the database again in a new session
    # and check if the data is present.
    conn_string = postgres_container.get_connection_url()
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, name FROM {test_table_name} WHERE id = 1;")
            result = cur.fetchone()
            assert result is not None, "Data should have been committed and be selectable."
            assert result[0] == 1
            assert result[1] == "test_name"


import io


def test_postgres_loader_rollback_on_exception(postgres_loader: PostgresLoader, postgres_container: PostgresContainer):
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
    conn_string = postgres_container.get_connection_url()
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            # A reliable way to check for table existence in PostgreSQL.
            cur.execute(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = %s);",
                (test_table_name,)
            )
            table_exists = cur.fetchone()[0]
            assert not table_exists, "Table should not exist after transaction was rolled back."


def test_postgres_loader_bulk_load(postgres_loader: PostgresLoader, postgres_container: PostgresContainer):
    """
    Integration test to verify that the bulk_load_stream method correctly
    loads data into the database using the COPY command.
    """
    test_table_name = "test_bulk_load_table"
    create_table_sql = f"CREATE TABLE {test_table_name} (id INT, name VARCHAR(100));"

    # Prepare sample CSV data in an in-memory bytes buffer.
    csv_data = b"1,first_row\\n2,second_row\\n3,third_row with comma\\,"
    data_stream = io.BytesIO(csv_data)

    with postgres_loader as loader:
        loader.execute_sql(create_table_sql)
        loader.bulk_load_stream(
            target_table=test_table_name,
            data_stream=data_stream,
            columns=["id", "name"],
            delimiter=","
        )

    # Verify the data was loaded correctly.
    conn_string = postgres_container.get_connection_url()
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {test_table_name};")
            count = cur.fetchone()[0]
            assert count == 3

            cur.execute(f"SELECT name FROM {test_table_name} WHERE id = 3;")
            name = cur.fetchone()[0]
            assert name == "third_row with comma,"
