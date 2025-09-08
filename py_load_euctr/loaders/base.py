from abc import ABC, abstractmethod
from typing import IO, ContextManager, TypeVar

ConnType = TypeVar("ConnType")


class BaseLoader(ABC):
    """Abstract Base Class for all database loaders (R.2.3.1).

    Defines the interface for database-specific loading mechanisms,
    ensuring adherence to the ELT architecture and native bulk loading principles.
    """

    @abstractmethod
    def get_conn(self) -> ContextManager[ConnType]:
        """Manages the database connection and transaction (R.2.3.2).

        Should be implemented as a context manager that yields a connection
        object. It must handle transaction semantics: committing on successful
        exit of the 'with' block and rolling back on any exception.
        """
        ...

    @abstractmethod
    def prepare_schema(self, table_name: str, schema_name: str, conn: ConnType) -> None:
        """Ensures the target schema and table exist (R.2.3.2).

        This method should be idempotent. It checks for the existence of the
        specified schema and table and creates them if they are not present.
        """
        ...

    @abstractmethod
    def bulk_load_stream(
        self,
        target_table: str,
        data_stream: IO[bytes],
        conn: ConnType,
    ) -> None:
        """Executes a native bulk load operation from an in-memory stream (R.2.3.2).

        This is a critical performance method and MUST be implemented using the
        target database's native bulk loading utility (e.g., PostgreSQL's
        COPY FROM STDIN). Standard SQL INSERTs are prohibited (R.5.2.1).

        Args:
            target_table: The fully qualified name of the table to load into.
            data_stream: A file-like object (e.g., io.BytesIO) containing the
                         data in a standard format like CSV.
            conn: An active database connection object.
        """
        ...

    @abstractmethod
    def execute_sql(self, sql: str, conn: ConnType) -> None:
        """Executes an arbitrary SQL command (R.2.3.2).

        Used for post-load transformations, MERGE operations, etc.

        Args:
            sql: The SQL string to execute.
            conn: An active database connection object.
        """
        ...
