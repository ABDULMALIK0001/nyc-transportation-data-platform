"""Validate a downloaded TLC Parquet file before Bronze storage."""

from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from src.common.exceptions import ValidationError
from src.common.logger import get_logger


logger = get_logger(__name__)

YELLOW_TRIP_REQUIRED_COLUMNS = frozenset(
    {
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "total_amount",
    }
)


@dataclass(frozen=True)
class ValidationResult:
    """Metadata read from a valid Parquet file."""

    row_count: int
    row_group_count: int
    column_count: int
    columns: tuple[str, ...]


def validate_yellow_trip_file(path: Path) -> ValidationResult:
    """Check file type, readability, rows, and required source columns."""
    if path.suffix.lower() != ".parquet":
        raise ValidationError(f"Expected a Parquet file, got: {path.name}")
    if not path.exists() or path.stat().st_size == 0:
        raise ValidationError(f"File is missing or empty: {path}")

    try:
        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.metadata
        columns = tuple(parquet_file.schema_arrow.names)
    except (OSError, ValueError) as exc:
        raise ValidationError(f"Unreadable Parquet file: {path}") from exc

    missing_columns = sorted(YELLOW_TRIP_REQUIRED_COLUMNS.difference(columns))
    if missing_columns:
        raise ValidationError(
            "Missing required columns: " + ", ".join(missing_columns)
        )
    if metadata.num_rows <= 0:
        raise ValidationError("The Parquet file contains no rows.")

    result = ValidationResult(
        row_count=metadata.num_rows,
        row_group_count=metadata.num_row_groups,
        column_count=len(columns),
        columns=columns,
    )
    logger.info(
        "Validated Parquet file: rows=%s columns=%s row_groups=%s",
        result.row_count,
        result.column_count,
        result.row_group_count,
    )
    return result

