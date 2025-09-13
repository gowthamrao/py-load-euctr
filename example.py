import asyncio
import io
import csv
import json
import uuid
import argparse
from datetime import datetime, timezone
from src.py_load_euctr.config import settings
from src.py_load_euctr.extractor import CtisExtractor
from src.py_load_euctr.loader.postgres import PostgresLoader
from src.py_load_euctr.models import CtisTrialBronze
from src.py_load_euctr.utils import get_last_decision_date


async def main(load_type: str):
    """
    Orchestrates the ELT process for fetching CTIS data and loading it
    into the Bronze layer of a PostgreSQL database.
    """
    print(f"Starting CTIS ELT process (mode: {load_type})...")
    load_id = str(uuid.uuid4())
    print(f"Generated Load ID: {load_id}")

    schema_name = "raw"
    table_name = "ctis_trials"

    # The loader is a context manager for the database connection
    # and transaction.
    with PostgresLoader(settings.db_connection_string) as loader:
        # 1. Ensure schema and table exist
        print(f"Ensuring '{schema_name}' schema and '{table_name}' table exist...")
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
        # Note: Using JSONB is highly recommended for storing raw JSON data.
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            _load_id VARCHAR(36) NOT NULL,
            _extracted_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
            _source_url TEXT,
            data JSONB
        );
        """
        loader.execute_sql(create_schema_sql)
        loader.execute_sql(create_table_sql)
        print("Database schema and table are ready.")

        # Determine the starting decision date for delta loads
        from_decision_date = None
        if load_type == "delta":
            from_decision_date = get_last_decision_date(settings.db_connection_string, schema_name, table_name)
            if from_decision_date is None:
                print("No last decision date found. Consider running a 'full' load first.")
                # Depending on requirements, you might want to stop here or default to a full load.
                # For this example, we'll stop.
                return

        # 2. Extract data and load it into the buffer
        extractor = CtisExtractor(settings)

        string_buffer = io.StringIO()
        writer = csv.writer(string_buffer, delimiter='\\t', quoting=csv.QUOTE_NONE, escapechar='\\\\')

        trials_processed = 0
        async for trial_data in extractor.extract_trials(from_decision_date=from_decision_date):
            source_url = extractor.RETRIEVE_URL_TEMPLATE.format(ct_number=trial_data.get("ctNumber", ""))

            bronze_record = CtisTrialBronze(
                load_id=load_id,
                extracted_at_utc=datetime.now(timezone.utc),
                source_url=source_url,
                data=trial_data,
            )

            # Serialize the JSON data part of the model to a string
            # The model dump will handle the datetime serialization correctly.
            model_dict = bronze_record.model_dump()
            model_dict['data'] = json.dumps(model_dict['data'])

            writer.writerow([
                model_dict['load_id'],
                model_dict['extracted_at_utc'].isoformat(),
                model_dict['source_url'],
                model_dict['data'],
            ])

            trials_processed += 1
            if trials_processed % 100 == 0:
                print(f"Extracted {trials_processed} trials...")

        print(f"Extraction complete. Total trials extracted: {trials_processed}")

        if trials_processed > 0:
            # 3. Load data from buffer into the database
            print("Loading data into PostgreSQL...")
            # Rewind the buffer to the beginning
            string_buffer.seek(0)

            # Encode the string buffer to bytes for the loader
            bytes_buffer = io.BytesIO(string_buffer.getvalue().encode('utf-8'))

            loader.bulk_load_stream(
                target_table=f"{schema_name}.{table_name}",
                data_stream=bytes_buffer,
                columns=["_load_id", "_extracted_at_utc", "_source_url", "data"],
                delimiter='\\t'
            )
            print(f"Successfully loaded {trials_processed} records into '{schema_name}.{table_name}'.")
        else:
            print("No trials were extracted, skipping load.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the CTIS ELT process.")
    parser.add_argument(
        "load_type",
        choices=["full", "delta"],
        help="Specify the type of load to perform: 'full' for a complete data refresh, 'delta' for an incremental update.",
    )
    args = parser.parse_args()

    asyncio.run(main(load_type=args.load_type))
