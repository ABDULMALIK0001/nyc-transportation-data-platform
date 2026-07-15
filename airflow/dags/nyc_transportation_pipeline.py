"""Monthly NYC transportation lakehouse pipeline."""

from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = "/opt/project"
PERIOD = (
    "--year {{ data_interval_start.year }} "
    "--month {{ data_interval_start.month }}"
)
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def project_task(task_id: str, command: str) -> BashOperator:
    """Create a task that runs one project command inside the Airflow image."""
    return BashOperator(
        task_id=task_id,
        bash_command=command,
        cwd=PROJECT_DIR,
        append_env=True,
        env={"PYTHONPATH": PROJECT_DIR},
    )


with DAG(
    dag_id="nyc_transportation_monthly",
    description="Bronze, Silver, Warehouse, and Gold monthly batch pipeline",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["data-engineering", "nyc", "batch"],
) as dag:
    ingest_trips = project_task(
        "ingest_yellow_taxi_trips", f"python -m src.main {PERIOD}"
    )
    ingest_zones = project_task(
        "ingest_taxi_zones", "python -m src.reference_pipeline"
    )
    ingest_weather = project_task(
        "ingest_hourly_weather", f"python -m src.weather_pipeline {PERIOD}"
    )
    build_silver = project_task(
        "build_silver_trips", f"python -m src.silver_pipeline {PERIOD}"
    )
    enrich_silver = project_task(
        "enrich_silver_trips", f"python -m src.enrichment_pipeline {PERIOD}"
    )
    load_warehouse = project_task(
        "load_postgres_warehouse", f"python -m src.warehouse_pipeline {PERIOD}"
    )
    build_gold = project_task(
        "build_and_test_gold",
        "dbt build --project-dir /opt/project/dbt --profiles-dir /opt/project/dbt",
    )

    ingest_trips >> build_silver
    [build_silver, ingest_zones, ingest_weather] >> enrich_silver
    enrich_silver >> load_warehouse >> build_gold
