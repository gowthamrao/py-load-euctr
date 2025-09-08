import importlib.resources

from jinja2 import Environment, FileSystemLoader

from py_load_euctr.loaders.base import BaseLoader


class Transformer:
    """Manages the SQL-based transformation from Bronze to Silver layer (R.4.4)."""

    def __init__(self, loader: BaseLoader):
        """Initializes the Transformer.

        Args:
            loader: A database loader instance that conforms to the BaseLoader interface.
        """
        self.loader = loader
        # Use importlib.resources to find the path to the sql templates directory,
        # which is a robust way to access package data.
        with importlib.resources.path("py_load_euctr", "sql") as sql_path:
            self.jinja_env = Environment(
                loader=FileSystemLoader(sql_path),
                autoescape=False,  # SQL is not HTML
            )

    def _execute_template(self, template_name: str, conn, **kwargs) -> None:
        """Renders a SQL template and executes it.

        Args:
            template_name: The name of the template file in the sql directory.
            conn: An active database connection.
            **kwargs: Variables to pass to the Jinja2 template.
        """
        template = self.jinja_env.get_template(template_name)
        sql = template.render(**kwargs)
        self.loader.execute_sql(sql, conn)

    def create_bronze_table(self, conn, schema: str, table: str) -> None:
        """Creates the Bronze layer table for raw data storage."""
        self._execute_template(
            "bronze/create_bronze_table.sql",
            conn,
            schema=schema,
            table=table,
        )

    def create_silver_tables(self, conn, schema: str) -> None:
        """Creates the normalized tables in the Silver layer."""
        self._execute_template("silver/create_silver_tables.sql", conn, schema=schema)

    def transform_bronze_to_silver(
        self,
        conn,
        bronze_schema: str,
        bronze_table: str,
        silver_schema: str,
        load_id: str,
    ) -> None:
        """Transforms data from Bronze to Silver using an UPSERT operation."""
        self._execute_template(
            "silver/upsert_trials.sql",
            conn,
            bronze_schema=bronze_schema,
            bronze_table=bronze_table,
            silver_schema=silver_schema,
            load_id=load_id,
        )
