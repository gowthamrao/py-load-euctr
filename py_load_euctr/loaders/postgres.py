import io
from contextlib import contextmanager
from typing import IO, Iterator

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from py_load_euctr.loaders.base import BaseLoader


class PostgresLoader(BaseLoader):
    """PostgreSQL implementation of the BaseLoader (R.5.3).

    This class provides a high-performance data loading mechanism for
    PostgreSQL, utilizing the native `COPY FROM STDIN` protocol as required
    by the FRD (R.5.2.1, R.5.3.1).
    """

    def __init__(self, dsn: str):
        """Initializes the PostgresLoader with connection details.

        Args:
            dsn: The connection string for the PostgreSQL database.
                 (e.g., "postgresql://user:password@host:port/dbname")
        """
        self.dsn = dsn

    @contextmanager
    def get_conn(self) -> Iterator[psycopg.Connection]:
        """Manages the PostgreSQL connection and transaction.

        Yields a connection object and handles commit on success or rollback on error.
        """
        with psycopg.connect(
            self.dsn,
            row_factory=dict_row,
        ) as conn, conn.transaction():
            yield conn

    def prepare_schema(
        self, table_name: str, schema_name: str, conn: psycopg.Connection
    ) -> None:
        """Ensures the target schema exists in PostgreSQL.

        Note: Table creation logic is separate. This only ensures the schema exists.
        """
        with conn.cursor() as cur:
            # Use psycopg.sql for safe dynamic identifier quoting
            create_schema_sql = sql.SQL(
                "CREATE SCHEMA IF NOT EXISTS {schema}",
            ).format(
                schema=sql.Identifier(schema_name),
            )
            cur.execute(create_schema_sql)

    def bulk_load_stream(
        self,
        target_table: str,
        data_stream: IO[bytes],
        conn: psycopg.Connection,
    ) -> None:
        """Uses `COPY FROM STDIN` to bulk load data into PostgreSQL (R.5.3.3).

        Args:
            target_table: The fully qualified name (e.g., "schema.table") to load into.
            data_stream: A file-like object containing CSV formatted data.
            conn: An active psycopg connection.
        """
        schema, table = target_table.split(".")
        qualified_table = sql.SQL("{}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
        )

        copy_sql = sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV)").format(
            qualified_table,
        )

        with conn.cursor() as cur, cur.copy(copy_sql) as copy:
            while data := data_stream.read(1024 * 1024):  # Read in 1MB chunks
                copy.write(data)

    def execute_sql(self, sql_string: str, conn: psycopg.Connection) -> None:
        """Executes an arbitrary SQL command.

        Args:
            sql_string: The SQL string to execute.
            conn: An active database connection object.
        """
        with conn.cursor() as cur:
            cur.execute(sql_string)
