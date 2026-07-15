"""Track PostgreSQL warehouse loads in the metadata schema."""

from uuid import UUID, uuid4

import psycopg

from src.common.exceptions import MetadataError
from src.config import Settings


class WarehouseLoadRepository:
    """Create and finalize one row for every warehouse load attempt."""

    def __init__(self, settings: Settings) -> None:
        self.dsn = settings.postgres_dsn

    def start_run(self, year: int, month: int, source_path: str) -> UUID:
        run_id = uuid4()
        statement = """
            INSERT INTO metadata.warehouse_load_runs (
                run_id, period_year, period_month, status, source_path
            )
            VALUES (%s, %s, %s, 'RUNNING', %s)
        """
        self._execute(statement, (run_id, year, month, source_path))
        return run_id

    def mark_success(
        self,
        run_id: UUID,
        source_rows: int,
        staging_rows: int,
        inserted_rows: int,
        existing_rows: int,
    ) -> None:
        statement = """
            UPDATE metadata.warehouse_load_runs
            SET status = 'SUCCESS',
                source_rows = %s,
                staging_rows = %s,
                inserted_rows = %s,
                existing_rows = %s,
                completed_at = CURRENT_TIMESTAMP,
                error_message = NULL
            WHERE run_id = %s
        """
        self._execute(
            statement,
            (source_rows, staging_rows, inserted_rows, existing_rows, run_id),
            require_one_row=True,
        )

    def mark_failed(self, run_id: UUID, error_message: str) -> None:
        statement = """
            UPDATE metadata.warehouse_load_runs
            SET status = 'FAILED',
                completed_at = CURRENT_TIMESTAMP,
                error_message = %s
            WHERE run_id = %s
        """
        self._execute(statement, (error_message[:4000], run_id), require_one_row=True)

    def _execute(
        self,
        statement: str,
        parameters: tuple[object, ...],
        require_one_row: bool = False,
    ) -> None:
        try:
            with psycopg.connect(self.dsn) as connection:
                cursor = connection.execute(statement, parameters)
                if require_one_row and cursor.rowcount != 1:
                    raise MetadataError("Warehouse metadata run was not found.")
        except psycopg.Error as exc:
            raise MetadataError(f"Could not update warehouse metadata: {exc}") from exc

