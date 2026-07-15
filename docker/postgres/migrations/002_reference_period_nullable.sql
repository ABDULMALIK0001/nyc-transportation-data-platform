ALTER TABLE metadata.ingestion_runs
    ALTER COLUMN period_year DROP NOT NULL,
    ALTER COLUMN period_month DROP NOT NULL;

