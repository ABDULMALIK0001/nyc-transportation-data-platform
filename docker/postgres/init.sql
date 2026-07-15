CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.ingestion_runs (
    run_id UUID PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    dataset_name VARCHAR(100) NOT NULL,
    period_year SMALLINT,
    period_month SMALLINT CHECK (period_month BETWEEN 1 AND 12),
    source_url TEXT NOT NULL,
    file_name TEXT NOT NULL,
    object_key TEXT NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    load_action VARCHAR(20) CHECK (load_action IN ('UPLOADED', 'SKIPPED')),
    file_size_bytes BIGINT,
    row_count BIGINT,
    file_checksum CHAR(64),
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_dataset_period
    ON metadata.ingestion_runs (dataset_name, period_year, period_month);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_status
    ON metadata.ingestion_runs (status, started_at DESC);

CREATE TABLE IF NOT EXISTS metadata.transformation_runs (
    run_id UUID PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    period_year SMALLINT NOT NULL,
    period_month SMALLINT NOT NULL CHECK (period_month BETWEEN 1 AND 12),
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    input_rows BIGINT,
    accepted_rows BIGINT,
    rejected_rows BIGINT,
    output_prefix TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_transformation_runs_job_period
    ON metadata.transformation_runs (job_name, period_year, period_month);
