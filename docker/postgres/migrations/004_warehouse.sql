CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS metadata.warehouse_load_runs (
    run_id UUID PRIMARY KEY,
    period_year SMALLINT NOT NULL,
    period_month SMALLINT NOT NULL CHECK (period_month BETWEEN 1 AND 12),
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    source_path TEXT NOT NULL,
    source_rows BIGINT,
    staging_rows BIGINT,
    inserted_rows BIGINT,
    existing_rows BIGINT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_location (
    location_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_id INTEGER NOT NULL UNIQUE,
    borough VARCHAR(100) NOT NULL,
    zone_name VARCHAR(200) NOT NULL,
    service_zone VARCHAR(100),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_month SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_weather (
    weather_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    weather_hour TIMESTAMP NOT NULL UNIQUE,
    temperature_celsius DOUBLE PRECISION,
    relative_humidity_percent DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    weather_code SMALLINT,
    wind_speed_kmh DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_payment_type (
    payment_type_key SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_type_id SMALLINT NOT NULL UNIQUE,
    payment_type_name VARCHAR(100) NOT NULL
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging.enriched_trips (
    record_id CHAR(64) NOT NULL,
    vendor_id SMALLINT,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count SMALLINT,
    trip_distance_miles DOUBLE PRECISION NOT NULL,
    pickup_location_id INTEGER NOT NULL,
    dropoff_location_id INTEGER NOT NULL,
    payment_type_id SMALLINT,
    fare_amount DOUBLE PRECISION,
    extra_amount DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    pickup_date DATE NOT NULL,
    pickup_hour TIMESTAMP NOT NULL,
    trip_duration_minutes DOUBLE PRECISION,
    average_speed_mph DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS warehouse.fact_trips (
    trip_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    record_id CHAR(64) NOT NULL UNIQUE,
    vendor_id SMALLINT,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    pickup_date_key INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
    pickup_location_key INTEGER NOT NULL REFERENCES warehouse.dim_location(location_key),
    dropoff_location_key INTEGER NOT NULL REFERENCES warehouse.dim_location(location_key),
    weather_key INTEGER NOT NULL REFERENCES warehouse.dim_weather(weather_key),
    payment_type_key SMALLINT REFERENCES warehouse.dim_payment_type(payment_type_key),
    passenger_count SMALLINT,
    trip_distance_miles DOUBLE PRECISION NOT NULL,
    trip_duration_minutes DOUBLE PRECISION,
    average_speed_mph DOUBLE PRECISION,
    fare_amount NUMERIC(12, 2),
    extra_amount NUMERIC(12, 2),
    mta_tax NUMERIC(12, 2),
    tip_amount NUMERIC(12, 2),
    tolls_amount NUMERIC(12, 2),
    improvement_surcharge NUMERIC(12, 2),
    congestion_surcharge NUMERIC(12, 2),
    airport_fee NUMERIC(12, 2),
    total_amount NUMERIC(12, 2),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_date
    ON warehouse.fact_trips (pickup_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_location
    ON warehouse.fact_trips (pickup_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_dropoff_location
    ON warehouse.fact_trips (dropoff_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_weather
    ON warehouse.fact_trips (weather_key);
