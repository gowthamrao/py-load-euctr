import psycopg
from psycopg import sql
from typing import IO, Any, Iterable, Optional

from .base import BaseLoader


class PostgresLoader(BaseLoader):
    """
    A database loader for PostgreSQL that uses the native COPY command.
    """

    def __init__(self, conn_string: str):
        """
        Initializes the loader with the database connection string.

        Args:
            conn_string: A libpq connection string (e.g., "dbname=test user=postgres").
        """
        self.conn_string = conn_string
        self.conn: Optional[psycopg.Connection] = None
        self.cursor: Optional[psycopg.Cursor] = None

    def __enter__(self) -> 'PostgresLoader':
        """
        Establishes the database connection and begins a transaction.
        """
        try:
            self.conn = psycopg.connect(self.conn_string, autocommit=False)
            self.cursor = self.conn.cursor()
        except psycopg.Error as e:
            print(f"Error connecting to PostgreSQL: {e}")
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Commits the transaction if no exceptions occurred, otherwise rolls back.
        Closes the database connection.
        """
        if not self.conn:
            return

        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            if self.cursor:
                self.cursor.close()
            self.conn.close()

    def bulk_load_stream(
        self,
        target_table: str,
        data_stream: IO[bytes],
        columns: Optional[list[str]] = None,
        delimiter: str = ',',
    ) -> None:
        """
        Executes a native bulk load operation using COPY FROM STDIN.
        """
        if not self.cursor:
            raise RuntimeError("Cursor is not available. The loader must be used as a context manager.")

        # Construct the COPY statement dynamically and safely.
        # Using sql.Identifier for the table and column names prevents SQL injection.
        if columns:
            column_sql = sql.SQL(" ({})").format(
                sql.SQL(", ").join(map(sql.Identifier, columns))
            )
        else:
            column_sql = sql.SQL("")

        copy_sql = sql.SQL("COPY {table}{columns} FROM STDIN WITH (FORMAT CSV, DELIMITER %(delim)s)").format(
            table=sql.Identifier(target_table),
            columns=column_sql
        )

        try:
            # The 'copy' object is a context manager for the COPY operation.
            with self.cursor.copy(copy_sql, {"delim": delimiter}) as copy:
                # To avoid loading the entire file into memory, read in chunks.
                while chunk := data_stream.read(8192):
                    copy.write(chunk)
        except psycopg.Error as e:
            print(f"Error during bulk load into '{target_table}': {e}")
            # Re-raise the exception to ensure the transaction is rolled back.
            raise

    def execute_sql(self, sql_query: str, params: Optional[Iterable[Any]] = None) -> None:
        """
        Executes an arbitrary SQL command.
        """
        if not self.cursor:
            raise RuntimeError("Cursor is not available. The loader must be used as a context manager.")

        try:
            self.cursor.execute(sql_query, params)
        except psycopg.Error as e:
            print(f"Error executing SQL: {e}")
            raise
