# py-load-euctr

This project extracts clinical trial data from the EU Clinical Trials Register (EUCTR) and loads it into a PostgreSQL database.

## Setup

This project uses `pdm` to manage dependencies.

1.  **Install `pdm`**:
    ```bash
    pip install pdm
    ```

2.  **Install dependencies**:
    ```bash
    pdm install
    ```

## Running Tests

To run the tests, use the following command:

```bash
pdm test
```

This will run `pytest` in the correct virtual environment.

## Linting

To lint the code, use the following command:

```bash
pdm lint
```

This will run `ruff` to check for code style issues.
