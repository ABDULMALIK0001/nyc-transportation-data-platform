"""Command-line pipeline for loading hourly NYC weather into Bronze."""

import argparse

from src.common.exceptions import PipelineError
from src.common.logger import get_logger
from src.config import settings
from src.ingestion.weather_extractor import WeatherExtractor
from src.storage.metadata_repository import MetadataRepository, RunIdentity
from src.storage.minio_client import MinioStorage
from src.validation.weather_validator import validate_weather_file


logger = get_logger(__name__)


def build_object_key(year: int, month: int, file_name: str) -> str:
    return f"weather/new_york_city/year={year}/month={month:02d}/{file_name}"


def run_pipeline(year: int, month: int, force_download: bool = False) -> None:
    """Extract, validate, and load one month of hourly weather."""
    logger.info("Starting weather pipeline for %s-%02d", year, month)
    extractor = WeatherExtractor(settings)
    source_url = extractor.build_source_url(year, month)
    file_name = f"weather_{year}-{month:02d}.json"
    object_key = build_object_key(year, month, file_name)
    metadata = MetadataRepository(settings)
    run_id = metadata.start_run(
        RunIdentity(
            source_name="Open-Meteo",
            dataset_name="nyc_hourly_weather",
            source_url=source_url,
            file_name=file_name,
            object_key=object_key,
            year=year,
            month=month,
        )
    )

    try:
        download = extractor.download(year, month, force=force_download)
        validation = validate_weather_file(download.local_path)
        upload = MinioStorage(settings).upload_bronze_file(
            local_path=download.local_path,
            object_key=object_key,
            sha256=download.sha256,
            source_url=download.source_url,
            row_count=validation.row_count,
            content_type="application/json",
        )
        metadata.mark_success(
            run_id=run_id,
            file_size_bytes=download.size_bytes,
            row_count=validation.row_count,
            file_checksum=download.sha256,
            skipped=upload.skipped,
        )
    except Exception as exc:
        try:
            metadata.mark_failed(run_id, str(exc))
        except PipelineError:
            logger.exception("Could not record failure for metadata run %s", run_id)
        if isinstance(exc, PipelineError):
            raise
        raise PipelineError(f"Unexpected weather pipeline failure: {exc}") from exc

    action = "already current" if upload.skipped else "uploaded"
    logger.info(
        "Weather pipeline finished: rows=%s object=%s/%s status=%s",
        validation.row_count,
        upload.bucket,
        upload.object_key,
        action,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest one month of hourly NYC weather into Bronze."
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--force-download", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    try:
        run_pipeline(arguments.year, arguments.month, arguments.force_download)
    except PipelineError as exc:
        logger.error("Pipeline failed: %s", exc)
        raise SystemExit(1) from exc
