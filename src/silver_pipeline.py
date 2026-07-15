"""Run the Spark Silver job, publish its outputs, and record metrics."""

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


def load_metrics(path: Path) -> dict[str, int]:
    """Read Spark metrics and verify that no input row disappeared."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        metrics = {
            "input_rows": int(payload["input_rows"]),
            "accepted_rows": int(payload["accepted_rows"]),
            "rejected_rows": int(payload["rejected_rows"]),
        }
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        raise PipelineError(f"Invalid Spark metrics file: {path}") from exc
    if metrics["input_rows"] != (
        metrics["accepted_rows"] + metrics["rejected_rows"]
    ):
        raise PipelineError("Spark metrics do not account for every input row.")
    return metrics


def run_pipeline(year: int, month: int) -> None:
    """Transform one Bronze month and publish accepted/rejected Silver data."""
    input_file = settings.download_dir / f"yellow_tripdata_{year}-{month:02d}.parquet"
    if not input_file.exists():
        raise PipelineError(
            f"Bronze source is not staged locally: {input_file}. Run ingestion first."
        )

    relative_output = Path("data") / "silver" / "yellow_taxi" / f"year={year}" / f"month={month:02d}"
    local_output = settings.project_root / relative_output
    object_prefix = f"yellow_taxi/year={year}/month={month:02d}"
    repository = TransformationRepository(settings)
    run_id = repository.start_run(
        "silver_yellow_taxi_trips", year, month, object_prefix
    )

    command = build_spark_submit_command(
        "spark/jobs/silver_trips.py",
        [
        "--input",
        f"/opt/project/data/downloads/{input_file.name}",
        "--output",
        "/opt/project/" + relative_output.as_posix(),
        "--year",
        str(year),
        "--month",
        str(month),
        ],
    )

    try:
        logger.info("Starting Spark Silver transformation for %s-%02d", year, month)
        result = subprocess.run(
            command,
            cwd=settings.project_root,
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
        if result.returncode != 0:
            error_output = (result.stderr or result.stdout)[-4000:]
            raise PipelineError(f"Spark job failed: {error_output}")
        metrics = load_metrics(local_output / "metrics.json")
        upload = MinioStorage(settings).upload_silver_directory(
            local_output, object_prefix
        )
        repository.mark_success(run_id=run_id, **metrics)
    except Exception as exc:
        try:
            repository.mark_failed(run_id, str(exc))
        except PipelineError:
            logger.exception("Could not record failed transformation run %s", run_id)
        if isinstance(exc, PipelineError):
            raise
        raise PipelineError(f"Unexpected Silver pipeline failure: {exc}") from exc

    logger.info(
        "Silver pipeline finished: input=%s accepted=%s rejected=%s "
        "uploaded_files=%s skipped_files=%s deleted_files=%s",
        metrics["input_rows"],
        metrics["accepted_rows"],
        metrics["rejected_rows"],
        upload.uploaded_files,
        upload.skipped_files,
        upload.deleted_files,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Silver Yellow Taxi data with Apache Spark."
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
