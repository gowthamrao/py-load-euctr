# Detailed Tutorial: Loading EUCTR Data with `py-load-euctr`

## 1. Introduction

This tutorial provides a comprehensive, step-by-step guide to using the `py-load-euctr` package. Its purpose is to extract clinical trial data from the EU Clinical Trials Register (EUCTR) and load it efficiently into a PostgreSQL database.

The package follows modern data engineering principles, landing the raw, unaltered JSON data into a "Bronze" table. This approach ensures that you have a complete and verbatim copy of the source data, which can then be used for downstream transformation and analysis (the "Silver" and "Gold" layers of the Medallion Architecture).

In this guide, you will learn how to:
- Set up your environment and configure the package.
- Understand the database schema used for loading.
- Perform an initial **full data load**.
- Keep your data up-to-date with **delta (incremental) loads**.
- Run basic queries to verify the loaded data.

## 2. Prerequisites

Before you begin, please ensure you have the following components set up and running:

- **Python (3.8 or newer):** The package is written in Python and requires a modern version.
- **PostgreSQL Server:** You need a running PostgreSQL instance that is network-accessible from where you will run the script.
- **`py-load-euctr` Package:** The package itself must be installed. It is recommended to do this within a virtual environment.

### Installation Steps

1.  **Clone the repository** (if you haven't already):
    ```bash
    git clone <repository-url>
    cd py-load-euctr
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install the package and its dependencies:**
    The `pip install .` command reads the `pyproject.toml` file and installs the package in "editable" mode, along with all necessary dependencies like `httpx` and `psycopg`.
    ```bash
    pip install .
    ```

## 3. Configuration

The application connects to your PostgreSQL database using credentials supplied via environment variables. This is a security best practice that avoids hardcoding secrets in the code.

Before running the script, you must set the following environment variables. The script will read them to construct the database connection string.

| Environment Variable | Description                      | Default Value |
| -------------------- | -------------------------------- | ------------- |
| `EUCTR_DB_HOST`      | The hostname of your PG server.  | `localhost`   |
| `EUCTR_DB_PORT`      | The port of your PG server.      | `5432`        |
| `EUCTR_DB_USER`      | The username for the connection. | `postgres`    |
| `EUCTR_DB_PASSWORD`  | The password for the connection. | `postgres`    |
| `EUCTR_DB_NAME`      | The database to connect to.      | `euctr`       |

### Example Configuration

Open your terminal and use the `export` command to set these variables. Replace the placeholder values with your actual database credentials.

```bash
# --- Database Connection Configuration ---

# The IP address or hostname of your PostgreSQL server
export EUCTR_DB_HOST=localhost

# The port your PostgreSQL server is listening on (5432 is the default)
export EUCTR_DB_PORT=5432

# Your PostgreSQL username
export EUCTR_DB_USER=myuser

# Your PostgreSQL password
export EUCTR_DB_PASSWORD='mysecretpassword'

# The name of the database you want to load data into
export EUCTR_DB_NAME=euctr_data
```

**Note:** These variables only persist for the current terminal session. For a more permanent solution, you can add them to your shell's profile file (e.g., `~/.bashrc`, `~/.zshrc`) or use a tool like `direnv`.

## 4. Database Setup and Schema

You do not need to create the database schema or table manually. The loading script handles this automatically on its first run.

On execution, the script ensures that a schema named `raw` and a table named `ctis_trials` exist in your target database. If they don't, the script will create them for you.

### Table Definition (DDL)

The script executes the following SQL to create the table:

```sql
CREATE TABLE IF NOT EXISTS raw.ctis_trials (
    _load_id VARCHAR(36) NOT NULL,
    _extracted_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    _source_url TEXT,
    data JSONB
);
```

### Column Descriptions

This table serves as the **Bronze Layer** in a Medallion Architecture. It is designed to hold the raw data with additional metadata for provenance and traceability.

| Column              | Description                                                                                                                                                             |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `_load_id`          | A unique identifier (UUID) for each run of the loading script. This allows you to trace every record back to the specific execution that loaded it.                         |
| `_extracted_at_utc` | A timestamp (with time zone) indicating exactly when the record was fetched from the source API. This is crucial for data lineage and debugging.                            |
| `_source_url`       | The exact API endpoint URL from which this specific trial's data was retrieved. This provides a direct link back to the source.                                            |
| `data`              | The complete, raw JSON payload for the clinical trial, stored in a PostgreSQL `JSONB` column. This is the core of the Bronze layer approach.                               |

### Why `JSONB`?

The use of `JSONB` (Binary JSON) is a key feature. Unlike `JSON` (which stores a plain text copy), `JSONB` stores the data in a decomposed binary format. This has two major advantages:

1.  **Querying Performance:** It is significantly faster to query and access nested elements within the JSON structure.
2.  **Indexing:** You can create GIN (Generalized Inverted Index) indexes on `JSONB` columns, which dramatically speeds up searches for specific keys or values within the JSON documents.

## 5. Running a Full Load

A "full load" is the process of extracting the entire dataset from the source and loading it into your database. This is the first operational step you will take after setting up your configuration.

### How to Run a Full Load

1.  Make sure your environment variables are set correctly (as described in Section 3).
2.  Ensure your PostgreSQL server is running and accessible.
3.  From your terminal, run the `example.py` script with the `full` argument:

    ```bash
    python example.py full
    ```

### What Happens During a Full Load?

When you execute this command, the script performs the following sequence of actions:

1.  **Generate Load ID:** A unique `_load_id` is created for this specific run.
2.  **Connect to Database:** It establishes a connection to your PostgreSQL database.
3.  **Verify Schema:** It runs the `CREATE SCHEMA` and `CREATE TABLE` commands (using `IF NOT EXISTS` to prevent errors on subsequent runs).
4.  **Extract Data:** The `CtisExtractor` begins fetching data from the EUCTR API. It starts from the first page and iterates through all available pages of results until the entire dataset has been downloaded.
5.  **Buffer In-Memory:** The extracted records are formatted into a tab-delimited structure and held in an in-memory buffer (`io.BytesIO`). This avoids writing temporary files to disk, improving performance.
6.  **Bulk Load with `COPY`:** Once extraction is complete, the `PostgresLoader` uses the highly efficient PostgreSQL `COPY` command to stream the data from the in-memory buffer directly into the `raw.ctis_trials` table. This is the fastest way to load large volumes of data into PostgreSQL.
7.  **Commit Transaction:** If the entire process is successful, the transaction is committed, and the data becomes visible in your table. If any part fails, the transaction is rolled back, leaving the database in its original state.

You will see log messages printed to the console, indicating the progress of the extraction and loading steps.

## 6. Running a Delta Load (Incremental Refresh)

After you have performed an initial full load, you don't need to re-fetch the entire dataset every time you want to check for new data. Instead, you can perform a "delta load" (also known as an incremental load). This process is much faster as it only fetches records that are new or have been updated since your last load.

**Prerequisite:** You must have successfully completed at least one `full` load before you can run a `delta` load.

### How to Run a Delta Load

1.  Make sure your environment variables are set and your database contains data from a previous load.
2.  From your terminal, run the `example.py` script with the `delta` argument:

    ```bash
    python example.py delta
    ```

### The High-Water Mark Mechanism

The delta load process works using a "high-water mark" strategy. The script identifies the most recent record you have already loaded and then asks the source API for only records that have appeared since then.

In this package, the `decisionDate` field from the trial data is used as the high-water mark.

### What Happens During a Delta Load?

1.  **Connect to Database:** The script connects to your PostgreSQL database.
2.  **Query for High-Water Mark:** This is the key step. The script executes a query to find the most recent `decisionDate` currently stored in your `raw.ctis_trials` table. The function `get_last_decision_date` in `example.py` runs the following SQL:
    ```sql
    SELECT (data->>'decisionDate')::date AS last_date
    FROM raw.ctis_trials
    WHERE data->>'decisionDate' IS NOT NULL
    ORDER BY last_date DESC
    LIMIT 1;
    ```
    This query extracts the `decisionDate` from the `JSONB` data, casts it to a date, and finds the latest one.

3.  **Extract New Data:** The most recent date found (e.g., `2023-10-26`) is then passed to the `CtisExtractor`. The extractor modifies its API request to only fetch trials with a `decisionDate` greater than this high-water mark.

4.  **Load New Records:** The script then proceeds with the same buffering and `COPY` process as a full load, but only for the small set of new records that were returned by the API. If no new records are found, the process finishes quickly without loading any data.

## 7. Verifying the Data

Once the loading process is complete, you can connect to your PostgreSQL database using a client like `psql`, DBeaver, or any other SQL tool to inspect the data.

Here are some example queries you can run to verify the load and explore the dataset.

### Count Total Records

First, check if you have data in the table.

```sql
SELECT COUNT(*) FROM raw.ctis_trials;
```

### Inspect a Single Record

To see the full structure of the data for a single trial, you can fetch one record. The `jsonb_pretty` function formats the JSON for readability.

```sql
SELECT
    _load_id,
    _extracted_at_utc,
    _source_url,
    jsonb_pretty(data) AS pretty_data
FROM
    raw.ctis_trials
LIMIT 1;
```

### Count Records per Load Run

To see how many records were loaded in each run (especially useful after a few delta loads), you can group by the `_load_id`.

```sql
SELECT _load_id, COUNT(*) AS number_of_records
FROM raw.ctis_trials
GROUP BY _load_id
ORDER BY MAX(_extracted_at_utc) DESC;
```

### Query Inside the `JSONB` Data

The real power of `JSONB` comes from the ability to query inside the JSON document itself. The `->>` operator extracts a JSON field as `text`.

**Example 1: Find a specific trial by its CT Number**

```sql
SELECT
    data ->> 'ctNumber' AS trial_number,
    data ->> 'trialStatus' AS status,
    data ->> 'sponsorName' AS sponsor
FROM
    raw.ctis_trials
WHERE
    data ->> 'ctNumber' = '2022-500024-16-00';
```

**Example 2: Find all trials sponsored by a specific organization**

```sql
SELECT
    data ->> 'ctNumber' AS trial_number,
    data ->> 'trialStatus' AS status
FROM
    raw.ctis_trials
WHERE
    data ->> 'sponsorName' ILIKE '%Janssen%';
```

**Example 3: Count trials by their status**

```sql
SELECT
    data ->> 'trialStatus' AS status,
    COUNT(*) AS count
FROM
    raw.ctis_trials
GROUP BY
    status
ORDER BY
    count DESC;
```

These queries demonstrate how you can begin to analyze the data directly from the Bronze layer table. For more complex analytics, you would typically transform this data into a more structured "Silver" layer.
