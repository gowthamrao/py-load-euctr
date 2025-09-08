-- (R.4.3) These tables store the cleansed, normalized, and query-optimized data.

-- The core 'trials' table, harmonizing data from both EudraCT and CTIS.
CREATE TABLE IF NOT EXISTS {{ schema }}.trials (
    trial_id TEXT PRIMARY KEY,
    title TEXT,
    protocol_id TEXT,
    status TEXT,
    start_date DATE,
    end_date DATE
);

-- Placeholder for other silver tables like sponsors, conditions, etc.
-- Example:
-- CREATE TABLE IF NOT EXISTS {{ schema }}.sponsors (
--     sponsor_id SERIAL PRIMARY KEY,
--     name TEXT NOT NULL,
--     address TEXT,
--     type TEXT,
--     UNIQUE(name, address)
-- );
--
-- CREATE TABLE IF NOT EXISTS {{ schema }}.trial_sponsors (
--     trial_id TEXT REFERENCES {{ schema }}.trials(trial_id),
--     sponsor_id INTEGER REFERENCES {{ schema }}.sponsors(sponsor_id),
--     PRIMARY KEY (trial_id, sponsor_id)
-- );
