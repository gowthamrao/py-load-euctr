import asyncio
import csv
import io
import json
import logging
import uuid
from datetime import date, datetime, timezone
from enum import Enum
from typing import IO, Any, Dict, Iterable

import httpx
import typer
import yaml
from pydantic import BaseModel

from py_load_euctr.extractor.euctr import EuctrExtractor
from py_load_euctr.loaders.postgres import PostgresLoader
from py_load_euctr.models.bronze import BronzeLayerRecord
from py_load_euctr.transformers.euctr import Transformer

# Basic structured logging setup (R.6.3.1)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class LoadMode(str, Enum):
    FULL = "FULL"
    DELTA = "DELTA"


class Settings(BaseModel):
    """Manages configuration from YAML and env vars (R.6.2.1)."""

    db_dsn: str = "postgresql://user:password@localhost:5432/euctr"
    bronze_schema: str = "raw"
    bronze_table: str = "euctr_trials"
    silver_schema: str = "silver"


def load_config(config_file: str | None) -> Dict[str, Any]:
    """Loads configuration from a YAML file."""
    if config_file:
        try:
            with open(config_file, "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning("Config file not found: %s", config_file)
    return {}


def _records_to_csv_stream(records: Iterable[Dict[str, Any]]) -> IO[bytes]:
    """Converts a list of dictionaries to an in-memory CSV byte stream."""
    buffer = io.StringIO()
    # Make a mutable copy to consume the iterator
    record_list = list(records)
    if not record_list:
        return io.BytesIO()

    fieldnames = record_list[0].keys()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()

    for record in record_list:
        # The 'data' field is a dict, it must be serialized to a JSON string
        # for proper storage in a single CSV column.
        if "data" in record and isinstance(record["data"], dict):
            record["data"] = json.dumps(record["data"])
        writer.writerow(record)

    buffer.seek(0)
    return io.BytesIO(buffer.read().encode("utf-8"))


async def arun_pipeline(
    load_mode: LoadMode = typer.Option(
        LoadMode.DELTA, help="Load mode: FULL or DELTA.", case_sensitive=False
    ),
    since_date: str = typer.Option(
        None, help="For DELTA loads, start date in YYYY-MM-DD format."
    ),
    db_dsn_override: str = typer.Option(None, help="Override database DSN."),
    config_file: str = typer.Option(
        "config.yaml", help="Path to YAML config file."
    ),
):
    """The main entry point to run the EUCTR ELT pipeline."""
    start_time = datetime.now(timezone.utc)
    load_id = str(uuid.uuid4())
    logger.info("Starting ELT pipeline run with load_id: %s", load_id)

    config = load_config(config_file)
    settings = Settings(**config)
    if db_dsn_override:
        settings.db_dsn = db_dsn_override

    loader = PostgresLoader(dsn=settings.db_dsn)
    transformer = Transformer(loader=loader)

    try:
        with loader.get_conn() as conn:
            logger.info("Preparing database schemas and tables...")
            transformer.create_bronze_table(
                conn, schema=settings.bronze_schema, table=settings.bronze_table
            )
            transformer.create_silver_tables(conn, schema=settings.silver_schema)

            async with httpx.AsyncClient(timeout=30.0) as client:
                extractor = EuctrExtractor(client=client)
                delta_since = (
                    date.fromisoformat(since_date)
                    if since_date and load_mode == LoadMode.DELTA
                    else None
                )

                logger.info("Starting data extraction (Mode: %s)...", load_mode.value)
                raw_records = [
                    rec async for rec in extractor.extract_trials(since_date=delta_since)
                ]

                if not raw_records:
                    logger.info("No new records found to load.")
                    return

                logger.info(
                    "Extracted %d records. Enriching and preparing for bulk load.",
                    len(raw_records),
                )
                now_utc = datetime.now(timezone.utc)
                bronze_records = [
                    BronzeLayerRecord(
                        load_id=load_id,
                        extracted_at_utc=now_utc,
                        loaded_at_utc=now_utc,
                        source_url=rec.get("url", ""),
                        package_version="0.1.0",
                        data=rec,
                    ).model_dump()
                    for rec in raw_records
                ]

                csv_stream = _records_to_csv_stream(bronze_records)
                logger.info(
                    "Loading data into Bronze table: %s.%s",
                    settings.bronze_schema,
                    settings.bronze_table,
                )
                loader.bulk_load_stream(
                    target_table=f"{settings.bronze_schema}.{settings.bronze_table}",
                    data_stream=csv_stream,
                    conn=conn,
                )
                logger.info("Bronze layer load complete.")

                logger.info("Transforming data from Bronze to Silver...")
                transformer.transform_bronze_to_silver(
                    conn=conn,
                    bronze_schema=settings.bronze_schema,
                    bronze_table=settings.bronze_table,
                    silver_schema=settings.silver_schema,
                    load_id=load_id,
                )
                logger.info("Silver layer transformation complete.")

    except Exception as e:
        logger.error("Pipeline failed: %s", e, exc_info=True)
        raise
    finally:
        duration = datetime.now(timezone.utc) - start_time
        logger.info("Pipeline run %s finished in %s.", load_id, duration)


# Create a synchronous wrapper for Typer, as it has issues with top-level async commands.
def main():
    app = typer.Typer()
    # This wrapper function is what Typer will call.
    def sync_wrapper(**kwargs):
        asyncio.run(arun_pipeline(**kwargs))

    # We re-create the Typer command with the same signature as arun_pipeline
    # but point it to our synchronous wrapper.
    app.command()(
        typer.main.create_command(
            sync_wrapper,
            name="arun-pipeline",
            help=arun_pipeline.__doc__,
            params=list(typer.main.get_params(arun_pipeline).values()),
        )
    )
    app()


if __name__ == "__main__":
    main()
