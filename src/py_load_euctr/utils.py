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
"""Utility functions for the application."""

from typing import Optional

from .loader.postgres import PostgresLoader


def get_last_decision_date(db_connection_string: str, schema: str, table: str) -> Optional[str]:
    """
    Retrieves the most recent decision date from the database.
    Returns the date in 'YYYY-MM-DD' format or None if no data exists.
    """
    print("Querying for the last decision date...")
    query = f"""
        SELECT (data->>'decisionDate')::date AS last_date
        FROM {schema}.{table}
        WHERE data->>'decisionDate' IS NOT NULL
        ORDER BY last_date DESC
        LIMIT 1;
    """
    try:
        # Use a new loader instance for this self-contained operation.
        with PostgresLoader(db_connection_string) as loader:
            result = loader.execute_sql(query, fetch="one")
            if result and result[0]:
                last_date = result[0].strftime('%Y-%m-%d')
                print(f"Found last decision date: {last_date}")
                return last_date
            else:
                print("No existing decision date found in the database.")
                return None
    except Exception as e:
        # This can happen if the table doesn't exist yet on the first run.
        print(f"Could not retrieve last decision date (table might not exist yet): {e}")
        return None
