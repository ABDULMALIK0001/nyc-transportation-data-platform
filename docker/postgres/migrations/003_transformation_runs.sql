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
