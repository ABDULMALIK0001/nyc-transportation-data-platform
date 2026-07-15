"""Command-line pipeline for loading Taxi Zone reference data into Bronze."""

import argparse

from src.common.exceptions import PipelineError
from src.common.logger import get_logger
from src.config import settings
from src.ingestion.taxi_zones_extractor import TaxiZonesExtractor
from src.storage.metadata_repository import MetadataRepository, RunIdentity
from src.storage.minio_client import MinioStorage
from src.validation.taxi_zones_validator import validate_taxi_zones_file


logger = get_logger(__name__)
OBJECT_KEY = "reference/taxi_zones/taxi_zone_lookup.csv"


def run_pipeline(force_download: bool = False) -> None:
    """Extract, validate, and load the non-periodic Taxi Zone lookup."""
    logger.info("Starting Taxi Zone reference pipeline")
    extractor = TaxiZonesExtractor(settings)
    metadata = MetadataRepository(settings)
    run_id = metadata.start_run(
        RunIdentity(
            source_name="NYC TLC",
            dataset_name="taxi_zones",
            source_url=settings.taxi_zone_lookup_url,
            file_name="taxi_zone_lookup.csv",
            object_key=OBJECT_KEY,
        )
    )

    try:
        download = extractor.download(force=force_download)
        validation = validate_taxi_zones_file(download.local_path)
        upload = MinioStorage(settings).upload_bronze_file(
            local_path=download.local_path,
            object_key=OBJECT_KEY,
            sha256=download.sha256,
            source_url=download.source_url,
            row_count=validation.row_count,
            content_type="text/csv",
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
        raise PipelineError(f"Unexpected reference pipeline failure: {exc}") from exc

    action = "already current" if upload.skipped else "uploaded"
    logger.info(
        "Taxi Zone pipeline finished: rows=%s object=%s/%s status=%s",
        validation.row_count,
        upload.bucket,
        upload.object_key,
        action,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest the NYC Taxi Zone lookup into Bronze."
    )
    parser.add_argument("--force-download", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    try:
        run_pipeline(force_download=arguments.force_download)
    except PipelineError as exc:
        logger.error("Pipeline failed: %s", exc)
        raise SystemExit(1) from exc

