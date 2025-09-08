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

import abc
from typing import IO, Any, Iterable, Optional


class BaseLoader(abc.ABC):
    """
    Abstract Base Class for all database loaders.

    This class defines the interface that all database-specific loaders must
    implement. It follows the Adapter Pattern to ensure database agnosticism
    and acts as a context manager to handle connection and transaction lifecycles.
    """

    @abc.abstractmethod
    def __enter__(self) -> "BaseLoader":
        """
        Establishes the database connection and begins a transaction.
        Returns the loader instance.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Commits the transaction if no exceptions occurred, otherwise rolls back.
        Closes the database connection.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def bulk_load_stream(
        self,
        target_table: str,
        data_stream: IO[bytes],
        columns: Optional[list[str]] = None,
        delimiter: str = ',',
    ) -> None:
        """
        Executes a native bulk load operation.

        This method should stream data from an in-memory buffer (e.g., io.BytesIO)
        directly to the database's native bulk loading utility (e.g., COPY).

        Args:
            target_table: The name of the table to load data into.
            data_stream: A file-like object (in-memory bytes buffer) containing
                         the data to be loaded in a format like CSV or TSV.
            columns: An optional list of column names if the data stream
                     does not map to all columns in the table or is in a
                     different order.
            delimiter: The delimiter used in the data stream.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def execute_sql(self, sql: str, params: Optional[Iterable[Any]] = None) -> None:
        """
        Executes an arbitrary SQL command.

        Used for tasks like creating schemas, tables, or running transformation
        logic (e.g., MERGE statements).

        Args:
            sql: The SQL statement to execute.
            params: An optional iterable of parameters to be used with the SQL
                    statement to prevent SQL injection.
        """
        raise NotImplementedError
