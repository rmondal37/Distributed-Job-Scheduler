CREATE TABLE jobs (
    id         UUID         PRIMARY KEY,
    type       VARCHAR(50)  NOT NULL,
    status     VARCHAR(50)  NOT NULL,
    payload    TEXT,
    created_at TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_jobs_status ON jobs (status);
