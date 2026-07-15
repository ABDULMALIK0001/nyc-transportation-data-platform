"""Command-line entry point for the first Bronze ingestion pipeline."""

import argparse

from src.common.exceptions import PipelineError
from src.common.logger import get_logger
from src.config import settings
from src.ingestion.trips_extractor import TripsExtractor
from src.storage.metadata_repository import MetadataRepository, RunIdentity
from src.storage.minio_client import MinioStorage
from src.validation.file_validator import validate_yellow_trip_file


logger = get_logger(__name__)


def build_object_key(year: int, month: int, file_name: str) -> str:
    """Build a partitioned Bronze key that remains stable across reruns."""
    return f"nyc_taxi/yellow/year={year}/month={month:02d}/{file_name}"


def run_pipeline(year: int, month: int, force_download: bool = False) -> None:
    """Extract, validate, and load one monthly file into Bronze."""
    logger.info("Starting Bronze pipeline for %s-%02d", year, month)
    extractor = TripsExtractor(settings)
    source_url = extractor.build_url(year, month, "yellow")
    file_name = source_url.rsplit("/", maxsplit=1)[-1]
    object_key = build_object_key(year, month, file_name)
    metadata = MetadataRepository(settings)
    run_id = metadata.start_run(
        RunIdentity(
            source_name="NYC TLC",
            dataset_name="yellow_taxi_trips",
            source_url=source_url,
            file_name=file_name,
            object_key=object_key,
            year=year,
            month=month,
        )
    )

    try:
        download = extractor.download(
            year=year,
            month=month,
            taxi_type="yellow",
            force=force_download,
        )
        validation = validate_yellow_trip_file(download.local_path)
        upload = MinioStorage(settings).upload_bronze_file(
            local_path=download.local_path,
            object_key=object_key,
            sha256=download.sha256,
            source_url=download.source_url,
            row_count=validation.row_count,
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
        raise PipelineError(f"Unexpected pipeline failure: {exc}") from exc

    status = "already current" if upload.skipped else "uploaded"
    logger.info(
        "Pipeline finished: rows=%s, Bronze object=%s/%s, status=%s",
        validation.row_count,
        upload.bucket,
        upload.object_key,
        status,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest one month of NYC yellow taxi data into Bronze."
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
