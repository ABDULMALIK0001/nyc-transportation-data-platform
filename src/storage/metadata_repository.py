"""Persist pipeline execution metadata in PostgreSQL."""

from dataclasses import dataclass
from uuid import UUID, uuid4

import psycopg

from src.common.exceptions import MetadataError
from src.common.logger import get_logger
from src.config import Settings


logger = get_logger(__name__)


@dataclass(frozen=True)
class RunIdentity:
    """Stable identifiers known before extraction begins."""

    source_name: str
    dataset_name: str
    source_url: str
    file_name: str
    object_key: str
    year: int | None = None
    month: int | None = None


class MetadataRepository:
    """Create and update one metadata row for every pipeline attempt."""

    def __init__(self, settings: Settings) -> None:
        self.dsn = settings.postgres_dsn

    def start_run(self, identity: RunIdentity) -> UUID:
        """Create a RUNNING record before any data movement begins."""
        run_id = uuid4()
        statement = """
            INSERT INTO metadata.ingestion_runs (
                run_id, source_name, dataset_name, period_year, period_month,
                source_url, file_name, object_key, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'RUNNING')
        """
        try:
            with psycopg.connect(self.dsn) as connection:
                connection.execute(
                    statement,
                    (
                        run_id,
                        identity.source_name,
                        identity.dataset_name,
                        identity.year,
                        identity.month,
                        identity.source_url,
                        identity.file_name,
                        identity.object_key,
                    ),
                )
        except psycopg.Error as exc:
            raise MetadataError(f"Could not create metadata run: {exc}") from exc
        logger.info("Created metadata run: %s", run_id)
        return run_id

    def mark_success(
        self,
        run_id: UUID,
        file_size_bytes: int,
        row_count: int,
        file_checksum: str,
        skipped: bool,
    ) -> None:
        """Finish a run and store the measurable ingestion outcome."""
        statement = """
            UPDATE metadata.ingestion_runs
            SET status = 'SUCCESS',
                load_action = %s,
                file_size_bytes = %s,
                row_count = %s,
                file_checksum = %s,
                completed_at = CURRENT_TIMESTAMP,
                error_message = NULL
            WHERE run_id = %s
        """
        action = "SKIPPED" if skipped else "UPLOADED"
        self._execute_update(
            statement,
            (action, file_size_bytes, row_count, file_checksum, run_id),
            "mark metadata run as successful",
        )
        logger.info("Metadata run %s finished with action=%s", run_id, action)

    def mark_failed(self, run_id: UUID, error_message: str) -> None:
        """Record a failed run without hiding its error."""
        statement = """
            UPDATE metadata.ingestion_runs
            SET status = 'FAILED',
                completed_at = CURRENT_TIMESTAMP,
                error_message = %s
            WHERE run_id = %s
        """
        self._execute_update(
            statement,
            (error_message[:4000], run_id),
            "mark metadata run as failed",
        )
        logger.info("Metadata run %s marked as FAILED", run_id)

    def _execute_update(
        self,
        statement: str,
        parameters: tuple[object, ...],
        operation: str,
    ) -> None:
        try:
            with psycopg.connect(self.dsn) as connection:
                cursor = connection.execute(statement, parameters)
                if cursor.rowcount != 1:
                    raise MetadataError(
                        f"Could not {operation}: run row was not found."
                    )
        except psycopg.Error as exc:
            raise MetadataError(f"Could not {operation}: {exc}") from exc
