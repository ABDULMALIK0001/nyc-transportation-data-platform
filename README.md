# NYC Transportation Data Platform

This project builds a monthly batch data pipeline for New York City Yellow Taxi
trips. It combines trip records with Taxi Zone reference data and hourly weather,
then produces tested analytical tables in PostgreSQL.

The project was built as a practical Data Engineering exercise. Its main focus
is data ingestion, transformation, modeling, orchestration, data quality, and
repeatable local infrastructure.

## Project goal

The goal is to turn raw files from different sources into reliable tables that
can be used for analysis. The pipeline is designed to:

- ingest monthly and reference datasets;
- preserve unchanged source data in a Bronze layer;
- clean, validate, and enrich millions of records with Spark;
- load a dimensional Data Warehouse in PostgreSQL;
- build tested Gold tables with dbt;
- schedule the workflow and support retries and backfills with Airflow;
- allow the same month to be rerun without duplicating data.

## Data sources

| Dataset | Format | Source | Purpose |
|---|---|---|---|
| Yellow Taxi trips | Parquet | NYC Taxi and Limousine Commission | Main monthly trip data |
| Taxi Zone lookup | CSV | NYC Taxi and Limousine Commission | Borough and zone names |
| Hourly weather | JSON API | Open-Meteo | Weather at each trip's pickup hour |

The current verified dataset is August 2024.

## Architecture

```text
NYC TLC and Open-Meteo
          |
          v
Python extraction and source validation
          |
          v
MinIO Bronze: unchanged source files
          |
          v
Apache Spark Silver: accepted, rejected, and enriched Parquet
          |
          v
PostgreSQL Warehouse: staging, dimensions, and trip facts
          |
          v
dbt Gold: tested analytical tables

Apache Airflow schedules and monitors the complete workflow.
```

More detail is available in [docs/architecture.md](docs/architecture.md).

## Technology stack

| Technology | Use in the project |
|---|---|
| Python 3.11 | Extraction, validation, storage operations, and warehouse loading |
| SQL | Warehouse DDL, dimensional loading, and analytical transformations |
| MinIO | Local S3-compatible storage for Bronze and Silver data |
| Apache Spark 4.1.2 | Cleaning, quality classification, deduplication, and enrichment |
| PostgreSQL 16 | Pipeline metadata, staging tables, star schema, and Gold tables |
| dbt Core 1.11.12 | Gold transformations, documentation, and data tests |
| Apache Airflow 3.3.0 | Monthly scheduling, task dependencies, retries, and backfills |
| Docker Compose | Reproducible local services and runtime isolation |
| Pytest | Unit tests for validation and pipeline behavior |

## Pipeline workflow

### 1. Ingestion

Python downloads the three source datasets over HTTPS. Files are first staged
under `data/downloads`, validated, and then uploaded to the MinIO Bronze bucket.
Unchanged objects are identified by SHA-256 checksum and skipped on reruns.

### 2. Silver cleaning

Spark standardizes column names and types, creates a full-row record identifier,
calculates trip duration and average speed, and classifies every source row as
accepted or rejected. Rejected rows are retained with an explicit reason rather
than silently removed.

The quality rules include missing timestamps, invalid date order, trips outside
the requested month, duration above six hours, distance above 100 miles,
negative amounts, invalid location identifiers, passenger counts outside 1-8,
and exact duplicate records.

### 3. Enrichment

Accepted trips are joined to the Taxi Zone lookup twice: once for pickup and
once for drop-off. They are also joined to weather by matching the pickup hour
to the corresponding weather hour. The job verifies that the joins do not
change the number of trip rows and reports every unmatched key.

### 4. Data Warehouse

Enriched Parquet is streamed into an unlogged PostgreSQL staging table using
`COPY`. Dimension keys are resolved in SQL before loading the fact table.

The star schema contains:

- `warehouse.fact_trips`
- `warehouse.dim_date`
- `warehouse.dim_location`
- `warehouse.dim_weather`
- `warehouse.dim_payment_type`

The complete data dictionary is in
[docs/data-dictionary.md](docs/data-dictionary.md).

### 5. Gold models

dbt builds five analytical tables:

- `gold.daily_trip_summary`
- `gold.monthly_revenue_summary`
- `gold.zone_performance`
- `gold.payment_type_summary`
- `gold.weather_trip_summary`

Source tests, model tests, and a reconciliation test run during every dbt build.

### 6. Orchestration

The `nyc_transportation_monthly` Airflow DAG connects the command-line pipelines
in dependency order. Each task retries twice with a five-minute delay, only one
monthly run can be active at a time, and historical periods can be processed
with an explicit backfill.

The DAG is paused when first created so that historical months are not loaded
accidentally.

## Data quality and repeatability

Quality is checked at several points rather than only at the end:

- source schema and file readability checks;
- accepted and rejected row reconciliation;
- unique Taxi Zone identifiers and weather hours;
- row counts before and after enrichment joins;
- source, staging, and warehouse row reconciliation;
- unique database constraints on trip record identifiers;
- dbt uniqueness, not-null, and Warehouse-to-Gold reconciliation tests.

Bronze checksum checks, deterministic Silver output, and Warehouse uniqueness
constraints make the monthly pipeline idempotent.

## Verified results

| Check | Result |
|---|---:|
| Source trip rows | 2,979,183 |
| Accepted Silver rows | 2,841,037 |
| Rejected Silver rows | 138,146 |
| Enriched Silver rows | 2,841,037 |
| Warehouse fact rows | 2,841,037 |
| Python tests | 18 passed |
| dbt models and tests | 47 passed |
| Airflow end-to-end DAG test | Success |

All accepted trips matched pickup zones, drop-off zones, and hourly weather.

## Project structure

```text
airflow/                  Airflow image and monthly DAG
dbt/                      Gold models, sources, and tests
docker/postgres/          PostgreSQL initialization and migrations
docs/                     Architecture and data dictionary
spark/jobs/               Spark cleaning and enrichment jobs
src/                      Python ingestion, storage, and warehouse pipelines
tests/                    Python unit tests
docker-compose.yml        Local services
requirements.txt          Local Python dependencies
```

Runtime data, virtual environments, logs, local secrets, and generated build
artifacts are excluded from Git.

## Running the project locally

Requirements:

- Docker Desktop
- Python 3.11
- Git
- at least 8 GB of memory available to Docker

Create and activate the virtual environment in PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create a local environment file and update it if necessary:

```powershell
Copy-Item .env.example .env
```

Build and start the services:

```powershell
docker compose build airflow-init
docker compose up airflow-init
docker compose up -d
```

Local interfaces:

- Airflow: `http://localhost:8080` (`airflow` / `airflow`)
- MinIO console: `http://localhost:9001` (`minioadmin` / `minioadmin`)
- PostgreSQL: `localhost:5432`, database `nyc_data`

These credentials are intended only for the local learning environment.

## Running one month manually

```powershell
python -m src.main --year 2024 --month 8
python -m src.reference_pipeline
python -m src.weather_pipeline --year 2024 --month 8
python -m src.silver_pipeline --year 2024 --month 8
python -m src.enrichment_pipeline --year 2024 --month 8
python -m src.warehouse_pipeline --year 2024 --month 8
dbt build --project-dir dbt --profiles-dir dbt
```

## Testing the Airflow workflow

The following logical date processes August 2024:

```powershell
docker compose exec airflow-scheduler airflow dags test nyc_transportation_monthly 2024-08-01
```

To inspect a backfill before starting it:

```powershell
docker compose exec airflow-scheduler airflow backfill create --dag-id nyc_transportation_monthly --from-date 2024-08-01 --to-date 2024-10-01 --max-active-runs 1 --dry-run
```

Remove `--dry-run` only after reviewing the interval and available disk space.

## Running quality checks

```powershell
pytest -q
dbt build --project-dir dbt --profiles-dir dbt
docker compose exec airflow-apiserver airflow dags list-import-errors
```

## What I learned

This project helped me understand how the separate parts of a data platform fit
together. I learned how to preserve raw data, define explainable quality rules,
process large Parquet datasets with Spark, and enrich facts with reference and
time-based data. I also gained practical experience with dimensional modeling,
fast PostgreSQL loading, dbt tests, idempotent reruns, and Airflow task design.

The most important lesson was that row reconciliation and operational metadata
should be designed into every stage of a pipeline, not added only after a data
problem appears.

## Future improvements

The next three improvements I would prioritize are:

1. **Cloud deployment:** replace MinIO with Amazon S3, move PostgreSQL to Amazon
   RDS, and run the orchestration and Spark workloads with managed services.
2. **Continuous integration:** add GitHub Actions to run Python tests, dbt
   validation, and static checks for every change before it is merged.
3. **Monitoring and advanced quality checks:** add failure alerts, pipeline
   metrics, and statistical anomaly checks for unusual speed, distance, and fare
   distributions.

## Stopping the services

```powershell
docker compose down
```

The command keeps the PostgreSQL and MinIO volumes. Adding `-v` deletes those
volumes and should only be used when the stored local data is no longer needed.
