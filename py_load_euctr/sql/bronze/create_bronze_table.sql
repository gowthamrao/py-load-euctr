-- (R.4.2) This table stores the raw, verbatim data from the source, plus metadata.
-- The 'data' column uses JSONB for efficient querying of raw attributes.
CREATE TABLE IF NOT EXISTS {{ schema }}.{{ table }} (
    load_id TEXT NOT NULL,
    extracted_at_utc TIMESTAMPTZ NOT NULL,
    loaded_at_utc TIMESTAMPTZ NOT NULL,
    source_url TEXT,
    package_version TEXT,
    record_hash VARCHAR(64),
    data JSONB NOT NULL
);

-- Optional: Add an index on the record_hash for faster lookups if using it for CDC
CREATE INDEX IF NOT EXISTS idx_{{ table }}_record_hash ON {{ schema }}.{{ table }} (record_hash);

-- Optional: Add an index on the source_url for debugging and traceability
CREATE INDEX IF NOT EXISTS idx_{{ table }}_source_url ON {{ schema }}.{{ table }} (source_url);
