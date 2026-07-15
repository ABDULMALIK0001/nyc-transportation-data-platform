# Data Dictionary

## Storage layers

| Layer | System | Purpose |
|---|---|---|
| Bronze | MinIO | Unchanged source files organized by dataset, year, and month |
| Silver | MinIO Parquet | Valid, rejected, standardized, and enriched records |
| Warehouse | PostgreSQL | Star schema with surrogate keys and enforced relationships |
| Gold | PostgreSQL/dbt | Small tested tables optimized for analytical queries |

## Warehouse tables

| Table | Grain | Main key |
|---|---|---|
| `warehouse.fact_trips` | One accepted taxi trip | `trip_key`, unique `record_id` |
| `warehouse.dim_date` | One calendar date | `date_key` in `YYYYMMDD` format |
| `warehouse.dim_location` | One NYC Taxi Zone | `location_key` |
| `warehouse.dim_weather` | One NYC weather hour | `weather_key` |
| `warehouse.dim_payment_type` | One TLC payment code | `payment_type_key` |

Important fact measures include passenger count, distance, duration, average
speed, fare, tip, tolls, surcharges, and total amount. Pickup and drop-off
locations use separate foreign keys to the same location dimension.

## Gold tables

| Table | Grain | Use |
|---|---|---|
| `gold.daily_trip_summary` | One pickup date | Daily volume, revenue, tips, distance, and duration |
| `gold.monthly_revenue_summary` | One year and month | Monthly revenue composition |
| `gold.zone_performance` | One pickup borough and zone | Zone demand and revenue performance |
| `gold.payment_type_summary` | One payment type | Payment mix, trip share, revenue, and tips |
| `gold.weather_trip_summary` | One weather condition | Compare trip behavior under simple weather groups |

## Operational metadata

| Table | Purpose |
|---|---|
| `metadata.ingestion_runs` | Source URL, period, checksum, size, rows, status, and upload action |
| `metadata.transformation_runs` | Spark input, accepted, rejected, output prefix, and status |
| `metadata.warehouse_load_runs` | Source, staging, inserted, existing rows, and status |

Airflow orchestration history is stored separately in the PostgreSQL `airflow`
database.
