"""Run Spark Silver enrichment and publish zones, weather, and trips."""

import argparse
import json
from pathlib import Path
import subprocess

from src.common.exceptions import PipelineError
from src.common.logger import get_logger
from src.common.spark_runner import build_spark_submit_command
from src.config import settings
from src.storage.minio_client import MinioStorage
from src.storage.transformation_repository import TransformationRepository


logger = get_logger(__name__)


def load_enrichment_metrics(path: Path) -> dict[str, int]:
    required = (
        "zone_rows",
        "weather_rows",
        "input_trip_rows",
        "enriched_trip_rows",
        "missing_pickup_zones",
        "missing_dropoff_zones",
        "missing_weather_hours",
    )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        metrics = {name: int(payload[name]) for name in required}
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        raise PipelineError(f"Invalid enrichment metrics file: {path}") from exc
    if metrics["input_trip_rows"] != metrics["enriched_trip_rows"]:
        raise PipelineError("Enrichment join changed the number of trip rows.")
    if metrics["zone_rows"] <= 0 or metrics["weather_rows"] <= 0:
        raise PipelineError("Enrichment dimensions are empty.")
    return metrics


def run_pipeline(year: int, month: int) -> None:
    """Build and publish all supporting Silver enrichment datasets."""
    trips_input = (
        settings.project_root
        / "data"
        / "silver"
        / "yellow_taxi"
        / f"year={year}"
        / f"month={month:02d}"
        / "accepted"
    )
    zones_input = settings.download_dir / "taxi_zone_lookup.csv"
    weather_input = settings.download_dir / f"weather_{year}-{month:02d}.json"
    for path in (trips_input, zones_input, weather_input):
        if not path.exists():
            raise PipelineError(f"Required Silver input is missing: {path}")

    zones_output = settings.project_root / "data" / "silver" / "taxi_zones"
    weather_output = (
        settings.project_root
        / "data"
        / "silver"
        / "weather"
        / f"year={year}"
        / f"month={month:02d}"
    )
    enriched_output = (
        settings.project_root
        / "data"
        / "silver"
        / "enriched_trips"
        / f"year={year}"
        / f"month={month:02d}"
    )
    enriched_prefix = f"enriched_trips/year={year}/month={month:02d}"
    repository = TransformationRepository(settings)
    run_id = repository.start_run(
        "silver_trip_enrichment", year, month, enriched_prefix
    )

    def container_path(path: Path) -> str:
        relative = path.relative_to(settings.project_root).as_posix()
        return f"/opt/project/{relative}"

    command = build_spark_submit_command(
        "spark/jobs/silver_enrichment.py",
        [
        "--trips",
        container_path(trips_input),
        "--zones",
        container_path(zones_input),
        "--weather",
        container_path(weather_input),
        "--zones-output",
        container_path(zones_output),
        "--weather-output",
        container_path(weather_output),
        "--enriched-output",
        container_path(enriched_output),
        "--year",
        str(year),
        "--month",
        str(month),
        ],
    )

    try:
        logger.info("Starting Silver enrichment for %s-%02d", year, month)
        result = subprocess.run(
            command,
            cwd=settings.project_root,
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
        if result.returncode != 0:
            error_output = (result.stderr or result.stdout)[-5000:]
            raise PipelineError(f"Spark enrichment failed: {error_output}")

        metrics = load_enrichment_metrics(enriched_output / "metrics.json")
        storage = MinioStorage(settings)
        zones_upload = storage.upload_silver_directory(
            zones_output, "reference/taxi_zones"
        )
        weather_upload = storage.upload_silver_directory(
            weather_output,
            f"weather/new_york_city/year={year}/month={month:02d}",
        )
        trips_upload = storage.upload_silver_directory(
            enriched_output, enriched_prefix
        )
        repository.mark_success(
            run_id=run_id,
            input_rows=metrics["input_trip_rows"],
            accepted_rows=metrics["enriched_trip_rows"],
            rejected_rows=0,
        )
    except Exception as exc:
        try:
            repository.mark_failed(run_id, str(exc))
        except PipelineError:
            logger.exception("Could not record failed enrichment run %s", run_id)
        if isinstance(exc, PipelineError):
            raise
        raise PipelineError(f"Unexpected enrichment failure: {exc}") from exc

    logger.info(
        "Enrichment finished: trips=%s zones=%s weather_hours=%s "
        "missing_pickup=%s missing_dropoff=%s missing_weather=%s "
        "published_files=%s",
        metrics["enriched_trip_rows"],
        metrics["zone_rows"],
        metrics["weather_rows"],
        metrics["missing_pickup_zones"],
        metrics["missing_dropoff_zones"],
        metrics["missing_weather_hours"],
        zones_upload.uploaded_files
        + weather_upload.uploaded_files
        + trips_upload.uploaded_files,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Silver dimensions and enriched Yellow Taxi trips."
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    try:
        run_pipeline(arguments.year, arguments.month)
    except PipelineError as exc:
        logger.error("Pipeline failed: %s", exc)
        raise SystemExit(1) from exc
