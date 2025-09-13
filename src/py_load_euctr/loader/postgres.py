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
"""Provides a PostgreSQL loader using the native COPY command."""

import types
from collections.abc import Iterable
from typing import IO, Any

import psycopg
from psycopg import sql

from .base import BaseLoader


class PostgresLoader(BaseLoader):
    """A database loader for PostgreSQL that uses the native COPY command."""

    def __init__(self, conn_string: str) -> None:
        """Initialize the loader with the database connection string.

        Args:
            conn_string: A libpq connection string (e.g., "dbname=test user=postgres").

        """
        self.conn_string = conn_string
        self.conn: psycopg.Connection | None = None
        self.cursor: psycopg.Cursor | None = None

    def __enter__(self) -> "PostgresLoader":
        """Establish the database connection and begin a transaction."""
        self.conn = psycopg.connect(self.conn_string, autocommit=False)
        self.cursor = self.conn.cursor()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Commit the transaction on success or roll back on error.

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
        columns: list[str] | None = None,
        delimiter: str = ",",
    ) -> None:
        """Execute a native bulk load operation using COPY FROM STDIN."""
        if not self.cursor:
            msg = (
                "Cursor is not available. "
                "The loader must be used as a context manager."
            )
            raise RuntimeError(msg)

        # Construct the COPY statement dynamically and safely.
        # Using sql.Identifier for the table and column names prevents SQL injection.
        if columns:
            column_sql = sql.SQL(" ({})").format(
                sql.SQL(", ").join(map(sql.Identifier, columns)),
            )
        else:
            column_sql = sql.SQL("")

        table_parts = target_table.split(".")
        if len(table_parts) == 2:
            table_sql = sql.SQL(".").join(map(sql.Identifier, table_parts))
        else:
            table_sql = sql.Identifier(target_table)

        copy_sql = sql.SQL(
            "COPY {table}{columns} FROM STDIN WITH (FORMAT CSV, DELIMITER %(delim)s)",
        ).format(
            table=table_sql,
            columns=column_sql,
        )

        # The 'copy' object is a context manager for the COPY operation.
        with self.cursor.copy(copy_sql, {"delim": delimiter}) as copy:
            # To avoid loading the entire file into memory, read in chunks.
            while chunk := data_stream.read(8192):
                copy.write(chunk)

    def execute_sql(
        self,
        sql_query: str,
        params: Iterable[Any] | None = None,
        fetch: str | None = None,
    ) -> Any:
        """Execute an arbitrary SQL command."""
        if not self.cursor:
            msg = (
                "Cursor is not available. "
                "The loader must be used as a context manager."
            )
            raise RuntimeError(msg)

        self.cursor.execute(sql_query, params)

        if fetch == "one":
            return self.cursor.fetchone()
        if fetch == "all":
            return self.cursor.fetchall()
        return None
