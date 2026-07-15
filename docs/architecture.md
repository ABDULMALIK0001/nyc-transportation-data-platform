# Architecture

## First pipeline

The first batch pipeline has three responsibilities:

1. Extract one monthly NYC TLC Parquet file over HTTPS.
2. Validate that the file is readable and follows the expected source contract.
3. Store the unchanged file in the MinIO Bronze bucket.

```text
NYC TLC source
      |
      v
TripsExtractor
      |
      v
Parquet validation
      |
      v
MinIO / bronze / nyc_taxi / yellow / year=YYYY / month=MM
```

The object key and SHA-256 metadata make reruns idempotent: a second run skips
the upload when the exact same file is already present.

Every attempt is also written to PostgreSQL before extraction begins:

```text
metadata.ingestion_runs
    RUNNING  ->  SUCCESS (UPLOADED or SKIPPED)
             ->  FAILED  (with error_message)
```

This control table records the source period, object key, checksum, file size,
row count, timestamps, and outcome for operational auditing.

## Reference data

The Taxi Zone lookup is ingested separately because it is non-periodic reference
data rather than a monthly fact dataset:

```text
Official Taxi Zone CSV
          |
          v
CSV contract and LocationID uniqueness checks
          |
          v
bronze/reference/taxi_zones/taxi_zone_lookup.csv
```

Its `LocationID` will later enrich trip pickup and drop-off identifiers with
borough and zone names in the Silver layer.

## Hourly weather data

The historical weather pipeline requests one calendar month of hourly New York
City weather from Open-Meteo, validates equal-length time series, and stores the
raw JSON by year and month:

```text
Open-Meteo archive API
          |
          v
Hourly JSON contract validation
          |
          v
bronze/weather/new_york_city/year=YYYY/month=MM/weather_YYYY-MM.json
```

Weather and trips use the same local timezone and monthly partitions so that
they can be joined by pickup hour in the Silver layer.

## Silver Yellow Taxi transformation

Apache Spark standardizes names and types, creates a full-row SHA-256 record ID,
calculates pickup date/hour, trip duration, and average speed, then classifies
every source row:

```text
Bronze Yellow Taxi Parquet
          |
          v
Spark standardization and derived columns
          |
          v
Data quality rules and exact-row duplicate detection
          |
          +----> silver/.../accepted/*.parquet
          |
          +----> silver/.../rejected/*.parquet (with rejection_reason)
```

The accepted and rejected counts must sum to the input count. Publishing uses
stable object keys and synchronizes the MinIO prefix so stale Spark parts cannot
remain after a rerun. Each attempt is recorded in
`metadata.transformation_runs`.

## Silver enrichment

Clean Taxi Zones and hourly weather are broadcast-joined to accepted trips. A
pickup and drop-off location join adds borough and zone names, while a pickup
hour join adds temperature, humidity, precipitation, weather code, and wind:

```text
accepted trips -----+
                    |
Taxi Zones ----------+--> enriched Silver trips
                    |
hourly weather ------+
```

The job asserts that the enriched row count equals the accepted trip count and
reports unmatched pickup zones, drop-off zones, and weather hours. For August
2024 all 2,841,037 accepted trips matched both dimensions.

## PostgreSQL Data Warehouse

Silver enriched trips are streamed into an unlogged staging table with
PostgreSQL COPY. Dimension keys are resolved in SQL before inserting facts:

```text
dim_date -----------+
dim_location -------+
dim_weather --------+--> fact_trips
dim_payment_type ---+
```

`fact_trips.record_id` is unique, foreign keys enforce dimensional integrity,
and the loader verifies source, staging, inserted, and existing row counts. A
month already containing the full Silver row count uses a fast skip before
COPY. Warehouse load attempts are stored in `metadata.warehouse_load_runs`.

## dbt Gold layer

dbt reads the Warehouse star schema and materializes five small analytical
tables in the PostgreSQL `gold` schema:

```text
warehouse.fact_trips --+--> gold.daily_trip_summary
warehouse dimensions --+--> gold.monthly_revenue_summary
                       +--> gold.zone_performance
                       +--> gold.payment_type_summary
                       +--> gold.weather_trip_summary
```

Source key tests check the Warehouse inputs. Model tests check uniqueness and
required fields, while a custom reconciliation test requires the sum of daily
Gold trips to equal the Warehouse fact count.

## Airflow orchestration

The `nyc_transportation_monthly` DAG connects all independent command-line
pipelines into one monthly workflow:

```text
trips ingestion -> Silver cleaning -----+
                                         |
zones ingestion -------------------------+-> Silver enrichment
                                         |          |
weather ingestion -----------------------+          v
                                             Warehouse load
                                                   |
                                                   v
                                             dbt Gold + tests
```

Independent ingestion tasks can run in parallel. The DAG allows one active run,
uses two retries with a five-minute delay, and supports explicit Airflow
backfills. It is paused on first creation to avoid unexpectedly loading all
historical periods.

Airflow uses its own `airflow` PostgreSQL database. The project data remains in
`nyc_data`, so orchestration metadata and business data are separated.

## Idempotency and observability

- Bronze uploads compare SHA-256 checksums and skip unchanged objects.
- Silver publishing synchronizes stable partition prefixes and removes stale
  Spark part files.
- Warehouse facts use a unique `record_id` and a complete-period fast skip.
- Gold tables are rebuilt and tested by dbt.
- PostgreSQL metadata tables audit ingestion, transformation, and Warehouse
  attempts.
- Airflow records task state, retries, logs, duration, and DAG run state.
