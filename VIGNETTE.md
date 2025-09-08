# Software vs. Functional Requirement Document (FRD) Vignette

**Package:** `py-load-euctr`
**Version:** 1.0 (FRD), 0.1.0 (Software)
**Date:** 2025-09-08

## Introduction

This document provides a detailed comparison of the `py-load-euctr` Python package (version 0.1.0) against its corresponding Functional Requirements Document (FRD version 1.0). The goal is to assess the current state of the software, identify areas of compliance, and highlight gaps where the implementation deviates from the specified requirements.

Each section mirrors the structure of the FRD for a direct, requirement-by-requirement analysis. The status of each key requirement is marked as **Met**, **Partially Met**, or **Not Met**.

---

## 2. System Architecture

The software architecture aligns well with the high-level design principles of the FRD, particularly regarding modularity and the ELT focus. The core logic is separated into an `extractor`, `loader`, and `models`.

### 2.1 Design Principles

- **ELT Focus:** **Met**. The `example.py` orchestrator clearly shows an Extract-Load process, with transformation intended to happen later.
- **Performance First:** **Met**. The critical requirement for native bulk loading is the central feature of the `PostgresLoader`.
- **Modularity and Cloud Agnosticism:** **Met**. The `BaseLoader` ABC provides a strong foundation for this.

### 2.2 High-Level Architecture

**Status:** **Partially Met**

The codebase reflects the proposed modular structure:
- `src/py_load_euctr/extractor.py`: The `Extractor` module.
- `src/py_load_euctr/models.py`: The `Models` module.
- `src/py_load_euctr/loader/`: The `Loader` abstraction and implementation.
- `example.py`: Serves as a temporary `Orchestrator/CLI`.

However, the `Transformer` module is completely absent, and the `parser.py` file is empty.

### 2.3 Database Adapter Pattern

**Status:** **Met**

The implementation of the Adapter Pattern for database loaders is a standout feature of the package and adheres perfectly to the FRD.

- **R.2.3.1 (BaseLoader ABC):** **Met**. An abstract base class is defined in `src/py_load_euctr/loader/base.py`.

  ```python
  # src/py_load_euctr/loader/base.py
  import abc

  class BaseLoader(abc.ABC):
      """
      Abstract Base Class for all database loaders.
      """
      # ... abstract methods defined here
  ```

- **R.2.3.2 (Interface Methods):** **Met**. The `BaseLoader` ABC specifies the full required interface.
    - **Connection/Transaction:** Handled elegantly via the `__enter__` and `__exit__` context manager methods.
    - **Bulk Loading:** The `bulk_load_stream` abstract method directly corresponds to the FRD requirement.
    - **Post-load SQL:** The `execute_sql` abstract method provides the necessary functionality for running transformations or DDL statements.

  ```python
  # src/py_load_euctr/loader/base.py
  @abc.abstractmethod
  def bulk_load_stream(
      self,
      target_table: str,
      data_stream: IO[bytes],
      # ... other params
  ) -> None:
      """
      Executes a native bulk load operation.
      """
      raise NotImplementedError

  @abc.abstractmethod
  def execute_sql(self, sql: str, params: Optional[Iterable[Any]] = None) -> None:
      """
      Executes an arbitrary SQL command.
      """
      raise NotImplementedError
  ```

- **R.2.3.3 (Specific Logic Containment):** **Met**. The design correctly isolates database-specific code. The generic interface is in `base.py`, while the PostgreSQL-specific implementation, including the `COPY` command syntax, is strictly contained within `postgres.py`. This modularity makes the system extensible to other databases as intended.

---

## 3. Functional Requirements: Data Acquisition (Extract)

The `CtisExtractor` class in `src/py_load_euctr/extractor.py` is responsible for data acquisition. It currently only supports the CTIS data source.

### 3.1 Retrieval Mechanism

- **R.3.1.1 (Systematic Retrieval):** **Met**. The extractor first hits a search endpoint to get a list of trials and then retrieves each one individually.
- **R.3.1.2 (Parsing Various Formats):** **Partially Met**. The implementation exclusively handles JSON data from the CTIS API (`response.json()`). It does not contain logic for parsing HTML or XML, which would be required for the EudraCT source.
- **R.3.1.3 (Asynchronous HTTP):** **Met**. The extractor correctly uses `httpx.AsyncClient` and `asyncio` to perform concurrent HTTP requests, maximizing extraction performance.

  ```python
  # src/py_load_euctr/extractor.py
  class CtisExtractor:
      def __init__(self, ...):
          self.client = client if client else httpx.AsyncClient(...)

      async def extract_trials(self) -> AsyncGenerator[Dict[str, Any], None]:
          # ...
          tasks = [self._get_full_trial_details(ct_number) for ct_number in ct_numbers]
          for future in asyncio.as_completed(tasks):
              trial_details = await future
              if trial_details:
                  yield trial_details
  ```

### 3.2 Delta Identification (CDC)

**Status:** **Not Met**

This is a major gap in the current implementation.
- **R.3.2.1 (Primary CDC - Timestamps):** **Not Met**. The extractor does not track any "last updated" timestamps.
- **R.3.2.2 (High-Water Mark):** **Not Met**. There is no logic to store or retrieve a high-water mark to limit queries to new records.
- **R.3.2.3 (Secondary CDC - Hashing):** **Not Met**. Record hashing is not implemented.

The current process performs a full extraction on every run.

### 3.3 Full Load Extraction

**Status:** **Met**

- **R.3.3.1 (Full Load Option):** The default and only behavior of the extractor is to perform a full load of the entire dataset, which satisfies this requirement.

### 3.4 Resilience and Politeness

**Status:** **Partially Met**

The implementation includes some basic resilience features but misses several key requirements.
- **R.3.4.1 (Error Handling):** **Partially Met**. The code includes basic `try...except` blocks for `httpx` errors but only prints the error and continues, which may not be a sufficiently robust strategy.
- **R.3.4.2 (Retries):** **Not Met**. There is no retry mechanism with exponential backoff.
- **R.3.4.3 (Rate Limiting):** **Not Met**. No rate limiting is implemented.
- **R.3.4.4 (User-Agent):** **Met**. A hardcoded User-Agent string is included in all requests.

  ```python
  # src/py_load_euctr/extractor.py
  self.client = client if client else httpx.AsyncClient(
      headers={"User-Agent": "py-load-euctr/0.1.0"},
      ...
  )
  ```

---

## 4. Functional Requirements: Data Representation and Transformation (Transform)

This area represents the largest gap between the FRD and the current software. The implementation focuses entirely on the Bronze layer, with the Silver layer and transformation logic being completely absent.

### 4.1 Data Architecture

**Status:** **Partially Met**

- **R.4.1.1 (Medallion Architecture):** The software currently only implements the Bronze layer of the Medallion Architecture.

### 4.2 Bronze Layer (Full Representation)

**Status:** **Partially Met**

The implementation of the Bronze layer is solid but incomplete.

- **R.4.2.1 (Verbatim Data Load):** **Met**. The `example.py` script loads the raw JSON payload into a single column.
- **R.4.2.2 (Flexible Data Type):** **Met**. The `CREATE TABLE` statement in `example.py` correctly uses `JSONB` for the `data` column, which is ideal for storing raw, schemaless JSON.

  ```sql
  -- from example.py
  CREATE TABLE IF NOT EXISTS raw.ctis_trials (
      _load_id VARCHAR(36) NOT NULL,
      _extracted_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
      _source_url TEXT,
      data JSONB
  );
  ```

- **R.4.2.3 (Provenance Metadata):** **Partially Met**. The `CtisTrialBronze` model and the database table include `_load_id`, `_extracted_at_utc`, and `_source_url`. However, they are missing `_loaded_at_utc`, `_package_version`, and `_record_hash`.

  ```python
  # src/py_load_euctr/models.py
  class CtisTrialBronze(BaseModel):
      load_id: str
      extracted_at_utc: datetime
      source_url: str
      data: Dict[str, Any]
  ```

- **R.4.2.4 (Append-Only):** **Met**. The current load process is append-only by nature.

### 4.3 Silver Layer (Standard Representation)

**Status:** **Not Met**

- **R.4.3.1 (Normalized Model):** **Not Met**. There is no code, SQL, or model definition for a normalized Silver layer.
- **R.4.3.2 (ER Structure):** **Not Met**. None of the specified entities (`Trials`, `Sponsors`, etc.) are implemented.

### 4.4 Transformation Logic

**Status:** **Not Met**

- **R.4.4.1 (Post-Load Transformation):** **Not Met**. No transformation logic exists.
- **R.4.4.2 (Idempotent Transformations):** **Not Met**. There are no `MERGE` statements or other UPSERT logic.
- **R.4.4.3 (Templated SQL):** **Not Met**. Since there is no transformation SQL, there are no templates.

### 4.5 Data Validation

**Status:** **Partially Met**

- **R.4.5.1 (Pydantic Validation):** **Met**. The `CtisTrialBronze` model is used in `example.py` to parse and validate the incoming data before it's written to the loading buffer.
- **R.4.5.2 (Quarantine for Failures):** **Not Met**. The script does not include a `try...except` block around the Pydantic model instantiation. A validation failure would currently cause the entire process to halt rather than quarantining the invalid record.

---

## 5. Functional Requirements: Data Loading (Load)

The data loading implementation is the most mature and well-executed part of the package, fully adhering to the FRD's critical performance requirements.

### 5.1 Loading Modes

**Status:** **Partially Met**

- **R.5.1.1 (`FULL` Mode):** **Met**. The `example.py` script implements a full load. Although it doesn't `TRUNCATE` the table first, the process is designed as a full refresh.
- **R.5.1.2 (`DELTA` Mode):** **Not Met**. This is not implemented due to the lack of CDC in the extractor.

### 5.2 Native Bulk Loading

**Status:** **Met**

This critical requirement is perfectly implemented.

- **R.5.2.1 (Mandatory Native Loading):** **Met**. The `PostgresLoader` exclusively uses the native `COPY` command and avoids standard `INSERT` statements for data loading.
- **R.5.2.2 (Standardized Intermediate Format):** **Met**. The `example.py` orchestrator processes the data into an in-memory, tab-delimited CSV format, which is a portable format suitable for bulk loading.

  ```python
  # example.py
  string_buffer = io.StringIO()
  # Use a tab as a delimiter to avoid issues with commas in JSON strings.
  writer = csv.writer(string_buffer, delimiter='\\t', ...)
  # ... write rows to buffer
  ```

- **R.5.2.3 (In-Memory Buffers):** **Met**. The entire process from extraction to loading is stream-oriented and avoids writing intermediate data to disk. The `example.py` script writes to an `io.StringIO` buffer, which is then encoded into an `io.BytesIO` buffer for the loader.

  ```python
  # example.py
  string_buffer.seek(0)
  bytes_buffer = io.BytesIO(string_buffer.getvalue().encode('utf-8'))
  loader.bulk_load_stream(..., data_stream=bytes_buffer, ...)
  ```

### 5.3 PostgreSQL Implementation (Default)

**Status:** **Met**

- **R.5.3.1 (COPY FROM STDIN):** **Met**. The loader uses `psycopg`'s `cursor.copy()` method, which directly implements the `COPY FROM STDIN` protocol.
- **R.5.3.2 (High-Performance Drivers):** **Met**. The project uses `psycopg[binary]>=3.1.18`, which is the recommended modern, high-performance driver.
- **R.5.3.3 (Direct Streaming):** **Met**. The `PostgresLoader` reads from the provided `data_stream` in chunks, streaming it directly into the `COPY` operation.

  ```python
  # src/py_load_euctr/loader/postgres.py
  def bulk_load_stream(...):
      # ...
      copy_sql = sql.SQL("COPY {table}{columns} FROM STDIN ...")
      with self.cursor.copy(copy_sql, ...) as copy:
          while chunk := data_stream.read(8192):
              copy.write(chunk)
  ```

### 5.4 Extensibility Requirements

**Status:** **Not Met**

While the `BaseLoader` ABC provides the necessary foundation for extensibility, no other database adapters (e.g., for Redshift, BigQuery, Databricks) have been implemented.

### 5.5 Transaction Management

**Status:** **Met**

- **R.5.5.1 (Atomic Operations):** **Met**. The `PostgresLoader` is implemented as a context manager. The `__enter__` method begins a transaction, and the `__exit__` method commits the transaction upon successful completion or rolls it back if any exception occurs during the process. This ensures that the entire load is an atomic unit.

---

## 6. Non-Functional Requirements

### 6.1 Performance and Scalability

**Status:** **Met**

- **R.6.1.1 & R.6.1.2:** The software is architected for performance. By prioritizing native bulk loading and using an asynchronous, streaming-based approach, the application layer's overhead is minimized. Performance is correctly bound by network and database capacity, not the Python code.

### 6.2 Configuration and Security

**Status:** **Partially Met**

- **R.6.2.1 (Configuration Management):** **Met**. The `config.py` module uses `pydantic-settings` to load configuration from environment variables, which is a robust and flexible method.
- **R.6.2.2 (Credentials Security):** **Partially Met**. Database credentials are included as default values in the `Settings` class. While this is not ideal, they can be securely overridden by environment variables. The FRD's recommendation to prioritize secure sources is followed in principle.
- **R.6.2.3 (SSL/TLS):** **Not specified**. The connection string does not enforce SSL, but `psycopg` will use it if the server requires it. This could be made more explicit.

### 6.3 Logging and Monitoring

**Status:** **Not Met**

- **R.6.3.1 (Structured Logging):** **Not Met**. The application uses `print()` statements for output instead of a structured logging library like `structlog`.
- **R.6.3.2 & R.6.3.3 (Traceability & KPIs):** **Not Met**. The logs do not contain sufficient detail for tracing or performance monitoring.

---

## 7. Development and Maintenance Standards

### 7.1 Packaging and Dependencies

**Status:** **Met**

- **R.7.1.1 (pyproject.toml):** **Met**. The project is configured with a `pyproject.toml` file.
- **R.7.1.2 (Dependency Management):** **Met**. The use of `pdm.lock` indicates that a modern dependency management tool (PDM) is being used to ensure reproducible environments.
- **R.7.1.3 (Optional Dependencies):** **Not Implemented**. As only one loader exists, there are no optional dependencies yet, but the structure supports this.

### 7.2 Code Quality

**Status:** **Met**

- **R.7.2.1 (Type Hinting):** **Met**. The codebase is fully type-hinted.
- **R.7.2.2 (Formatting/Linting):** **Met**. The `pyproject.toml` file lists `black`, `ruff`, and `mypy` as development dependencies, indicating these tools are used to enforce code quality.
- **R.7.2.3 (Pre-commit Hooks):** **Not specified**. There is no `.pre-commit-config.yaml` file in the repository.

### 7.3 Testing Strategy

**Status:** **Partially Met**

The project has a `tests` directory with tests for the extractor and loader, indicating a commitment to the testing strategy.
- **R.7.3.1 (Unit Tests):** **Partially Met**. `tests/test_extractor.py` exists, but coverage is unknown.
- **R.7.3.2 (Mocking HTTP):** **Met**. The `pytest-httpx` dependency suggests external requests are mocked.
- **R.7.3.3 (Integration Tests):** **Partially Met**. The presence of `tests/test_loader_postgres.py` and the `testcontainers` dependency strongly implies that integration tests against a real database are being performed, which is a critical requirement.

### 7.4 CI/CD and Documentation

**Status:** **Not Met**

- **R.7.4.1 (CI Pipeline):** **Not Met**. There is no CI configuration file (e.g., in a `.github/workflows` directory).
- **R.7.4.2 (Documentation):** **Not Met**. Apart from this vignette, there is no formal documentation (e.g., Sphinx or MkDocs).

---

## 8. Conclusion

The `py-load-euctr` package in its current state is a strong proof-of-concept for the most critical performance requirement: high-throughput data ingestion using native database utilities. The implementation of the database adapter pattern and the PostgreSQL loader is robust, extensible, and directly aligned with the FRD.

However, the project is far from complete. The scope needs to be expanded to include the EudraCT data source, and several major functional and non-functional requirements are missing. The most significant gaps are:
1.  **Lack of a Silver Layer:** There is no transformation logic or normalized data model.
2.  **No Delta Loading:** The system cannot perform incremental updates.
3.  **Incomplete Resilience:** The extractor is missing key features like retries and rate limiting.
4.  **Production-Readiness:** The absence of structured logging, CI/CD, and formal documentation hinders its use in a production environment.

The current codebase serves as an excellent foundation for the "E" and "L" in the ELT process for the Bronze layer. Future development should prioritize the "T" (Transformation to Silver), delta loading capabilities, and bringing the non-functional requirements up to the specified standard.
