# Using `py-load-euctr` to Load Clinical Trial Data

This vignette provides a step-by-step guide on how to use the `py-load-euctr` package to extract clinical trial data from the EU Clinical Trials Register (EUCTR) and load it into a PostgreSQL database.

## 1. Installation

The package and its dependencies can be installed using `pip`. It is recommended to use a virtual environment.

```bash
# Create and activate a virtual environment (optional but recommended)
python -m venv .venv
source .venv/bin/activate

# Install the package from the root of the repository
pip install .
```

The main dependencies are:
- `httpx`: For making HTTP requests to the EUCTR API.
- `pydantic`: For data validation and settings management.
- `beautifulsoup4`: For parsing HTML (if needed, though this package uses the JSON API).
- `psycopg[binary]`: The PostgreSQL database adapter for Python.

## 2. Configuration

The database connection is configured using environment variables. The package will automatically read these variables to construct the connection string.

The following environment variables are supported:

- `EUCTR_DB_HOST`: The hostname of your PostgreSQL server (default: `localhost`).
- `EUCTR_DB_PORT`: The port of your PostgreSQL server (default: `5432`).
- `EUCTR_DB_USER`: The username for the database connection (default: `postgres`).
- `EUCTR_DB_PASSWORD`: The password for the database connection (default: `postgres`).
- `EUCTR_DB_NAME`: The name of the database to connect to (default: `euctr`).

Before running the loading script, make sure to set these variables, for example:

```bash
export EUCTR_DB_HOST=your-db-host
export EUCTR_DB_USER=your-db-user
export EUCTR_DB_PASSWORD=your-db-password
export EUCTR_DB_NAME=your-db-name
```

## 3. Running the ELT Process

The main script for running the extract, load, and transform (ELT) process is `example.py`. It can be run in two modes: `full` and `delta`.

### Full Load

A full load fetches all clinical trial data from the EUCTR API. This is the recommended first step to populate your database.

**Steps:**

1.  **Ensure your PostgreSQL database is running and accessible.**
2.  **Set the environment variables** as described in the "Configuration" section.
3.  **Run the `example.py` script with the `full` argument:**

    ```bash
    python example.py full
    ```

**What it does:**

- **Connects to the PostgreSQL database.**
- **Creates the `raw` schema** if it doesn't exist.
- **Creates the `ctis_trials` table** if it doesn't exist. This table is designed to store the raw JSON data from the API in a `JSONB` column, which is efficient for querying.
- **Fetches all trials** from the EUCTR API, handling pagination automatically.
- **Loads the data in bulk** into the `raw.ctis_trials` table.

### Delta Load

A delta load (or incremental load) fetches only the trials that have been added or updated since the last load. This is done by filtering trials based on their `decisionDate`.

To perform a delta load, you must have already completed at least one full load.

**Steps:**

1.  **Ensure your PostgreSQL database is running and contains data from a previous load.**
2.  **Set the environment variables** as described in the "Configuration" section.
3.  **Run the `example.py` script with the `delta` argument:**

    ```bash
    python example.py delta
    ```

**What it does:**

- **Connects to the PostgreSQL database.**
- **Queries the `raw.ctis_trials` table** to find the most recent `decisionDate`.
- **Fetches only the trials** from the EUCTR API with a `decisionDate` after the most recent one in your database.
- **Loads the new data** into the `raw.ctis_trials` table.

This allows you to keep your database up-to-date without having to re-download the entire dataset each time.

## Example Code (`example.py`)

The `example.py` script orchestrates the entire process. Here is a summary of its key components:

- **`get_last_decision_date()`**: This function queries your database to find the last decision date, which is crucial for delta loads.
- **`main()`**: The main asynchronous function that:
    - Establishes a database connection.
    - Ensures the necessary schema and table exist.
    - Determines whether to perform a full or delta load based on the command-line argument.
    - Initializes the `CtisExtractor` to fetch data.
    - Processes the data into a structured format.
    - Uses the `PostgresLoader` to bulk-load the data into the database.
- **Argument Parsing**: The script uses `argparse` to handle the `full` and `delta` command-line arguments.

By following this guide, you should be able to successfully install, configure, and run the `py-load-euctr` package to maintain a local copy of the EU Clinical Trials Register data.
