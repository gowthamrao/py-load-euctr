-- (R.4.4.2) This query performs an idempotent transformation from Bronze to Silver.
-- It uses an "UPSERT" pattern (INSERT ... ON CONFLICT) to either insert new trials
-- or update existing ones based on the primary key.
INSERT INTO {{ silver_schema }}.trials (
    trial_id,
    title,
    protocol_id,
    status,
    start_date,
    end_date
)
SELECT
    -- These JSON operators (->>) extract fields from the 'data' column.
    -- The specific field names are placeholders and will depend on the
    -- actual structure of the source data.
    raw.data->>'eudract_number' AS trial_id,
    raw.data->>'full_title' AS title,
    raw.data->>'sponsor_protocol_number' AS protocol_id,
    raw.data->>'trial_status' AS status,
    (raw.data->>'date_of_competent_authority_decision')::date AS start_date,
    (raw.data->>'trial_end_date')::date AS end_date
FROM
    {{ bronze_schema }}.{{ bronze_table }} AS raw
-- The WHERE clause ensures we only process records from the current batch.
WHERE
    raw.load_id = '{{ load_id }}'
ON CONFLICT (trial_id) DO UPDATE SET
    -- If a trial with the same ID already exists, this clause updates its fields.
    -- 'EXCLUDED' refers to the values from the new row that was proposed for insertion.
    title = EXCLUDED.title,
    protocol_id = EXCLUDED.protocol_id,
    status = EXCLUDED.status,
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date
;
