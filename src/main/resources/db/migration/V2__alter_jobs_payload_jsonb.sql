ALTER TABLE jobs
    ALTER COLUMN payload TYPE jsonb USING payload::jsonb,
    ALTER COLUMN payload SET NOT NULL;
