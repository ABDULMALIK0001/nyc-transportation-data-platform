"""Command-line entry point for loading Silver into PostgreSQL Warehouse."""

import argparse

from src.common.exceptions import PipelineError
from src.common.logger import get_logger
from src.config import settings
from src.warehouse.load_repository import WarehouseLoadRepository
from src.warehouse.loader import WarehouseLoader, WarehouseLoadResult


logger = get_logger(__name__)


def validate_load_result(result: WarehouseLoadResult) -> None:
    """Require complete staging and idempotent inserted/existing accounting."""
    if result.staging_rows not in {0, result.source_rows}:
        raise PipelineError("Warehouse staging does not match the Silver source.")
    if result.staging_rows == 0 and result.inserted_rows != 0:
        raise PipelineError("Warehouse cannot insert rows when staging was skipped.")
    if result.source_rows != result.inserted_rows + result.existing_rows:
        raise PipelineError("Warehouse load does not account for every source row.")


def run_pipeline(year: int, month: int) -> None:
    source_path = (
        settings.project_root
        / "data"
        / "silver"
        / "enriched_trips"
        / f"year={year}"
        / f"month={month:02d}"
    )
    repository = WarehouseLoadRepository(settings)
    run_id = repository.start_run(year, month, str(source_path))
    logger.info("Starting PostgreSQL Warehouse load for %s-%02d", year, month)
    try:
        result = WarehouseLoader(settings).load(year, month)
        validate_load_result(result)
        repository.mark_success(
            run_id=run_id,
            source_rows=result.source_rows,
            staging_rows=result.staging_rows,
            inserted_rows=result.inserted_rows,
            existing_rows=result.existing_rows,
        )
    except Exception as exc:
        try:
            repository.mark_failed(run_id, str(exc))
        except PipelineError:
            logger.exception("Could not record failed Warehouse run %s", run_id)
        if isinstance(exc, PipelineError):
            raise
        raise PipelineError(f"Unexpected Warehouse load failure: {exc}") from exc

    logger.info(
        "Warehouse load finished: source=%s staging=%s inserted=%s existing=%s",
        result.source_rows,
        result.staging_rows,
        result.inserted_rows,
        result.existing_rows,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load one Silver month into the PostgreSQL star schema."
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
