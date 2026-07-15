"""Persist Silver transformation execution metadata in PostgreSQL."""

from uuid import UUID, uuid4

import psycopg

from src.common.exceptions import MetadataError
from src.common.logger import get_logger
from src.config import Settings


logger = get_logger(__name__)


class TransformationRepository:
    """Track Spark jobs independently from source ingestion attempts."""

    def __init__(self, settings: Settings) -> None:
        self.dsn = settings.postgres_dsn

    def start_run(
        self,
        job_name: str,
        year: int,
        month: int,
        output_prefix: str,
    ) -> UUID:
        run_id = uuid4()
        statement = """
            INSERT INTO metadata.transformation_runs (
                run_id, job_name, period_year, period_month, status, output_prefix
            )
            VALUES (%s, %s, %s, %s, 'RUNNING', %s)
        """
        self._execute(
            statement,
            (run_id, job_name, year, month, output_prefix),
            "create transformation run",
        )
        logger.info("Created transformation run: %s", run_id)
        return run_id

    def mark_success(
        self,
        run_id: UUID,
        input_rows: int,
        accepted_rows: int,
        rejected_rows: int,
    ) -> None:
        statement = """
            UPDATE metadata.transformation_runs
            SET status = 'SUCCESS',
                input_rows = %s,
                accepted_rows = %s,
                rejected_rows = %s,
                completed_at = CURRENT_TIMESTAMP,
                error_message = NULL
            WHERE run_id = %s
        """
        self._execute(
            statement,
            (input_rows, accepted_rows, rejected_rows, run_id),
            "mark transformation successful",
            require_one_row=True,
        )

    def mark_failed(self, run_id: UUID, error_message: str) -> None:
        statement = """
            UPDATE metadata.transformation_runs
            SET status = 'FAILED',
                completed_at = CURRENT_TIMESTAMP,
                error_message = %s
            WHERE run_id = %s
        """
        self._execute(
            statement,
            (error_message[:4000], run_id),
            "mark transformation failed",
            require_one_row=True,
        )

    def _execute(
        self,
        statement: str,
        parameters: tuple[object, ...],
        operation: str,
        require_one_row: bool = False,
    ) -> None:
        try:
            with psycopg.connect(self.dsn) as connection:
                cursor = connection.execute(statement, parameters)
                if require_one_row and cursor.rowcount != 1:
                    raise MetadataError(f"Could not {operation}: run row was not found.")
        except psycopg.Error as exc:
            raise MetadataError(f"Could not {operation}: {exc}") from exc
