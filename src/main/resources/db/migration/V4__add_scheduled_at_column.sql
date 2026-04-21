ALTER TABLE jobs
    ADD COLUMN scheduled_at TIMESTAMP;

CREATE INDEX idx_jobs_scheduled_at ON jobs (scheduled_at)
    WHERE status = 'PENDING' AND scheduled_at IS NOT NULL;
