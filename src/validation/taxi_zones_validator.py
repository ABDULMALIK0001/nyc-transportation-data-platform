"""Validate the Taxi Zone CSV before storing it in Bronze."""

import csv
from dataclasses import dataclass
from pathlib import Path

from src.common.exceptions import ValidationError
from src.common.logger import get_logger


logger = get_logger(__name__)
REQUIRED_COLUMNS = ("LocationID", "Borough", "Zone", "service_zone")


@dataclass(frozen=True)
class TaxiZonesValidationResult:
    """Profile of a valid Taxi Zone reference file."""

    row_count: int
    columns: tuple[str, ...]
    unique_location_count: int


def validate_taxi_zones_file(path: Path) -> TaxiZonesValidationResult:
    """Check the CSV contract and require a unique integer LocationID."""
    if path.suffix.lower() != ".csv":
        raise ValidationError(f"Expected a CSV file, got: {path.name}")
    if not path.exists() or path.stat().st_size == 0:
        raise ValidationError(f"File is missing or empty: {path}")

    location_ids: set[int] = set()
    row_count = 0
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as input_file:
            reader = csv.DictReader(input_file)
            columns = tuple(reader.fieldnames or ())
            missing = [column for column in REQUIRED_COLUMNS if column not in columns]
            if missing:
                raise ValidationError("Missing required columns: " + ", ".join(missing))

            for line_number, row in enumerate(reader, start=2):
                row_count += 1
                try:
                    location_id = int(row["LocationID"])
                except (TypeError, ValueError) as exc:
                    raise ValidationError(
                        f"Invalid LocationID on CSV line {line_number}."
                    ) from exc
                if location_id in location_ids:
                    raise ValidationError(
                        f"Duplicate LocationID {location_id} on CSV line {line_number}."
                    )
                if not row["Borough"].strip() or not row["Zone"].strip():
                    raise ValidationError(
                        f"Blank Borough or Zone on CSV line {line_number}."
                    )
                location_ids.add(location_id)
    except UnicodeDecodeError as exc:
        raise ValidationError(f"Taxi Zone CSV is not valid UTF-8: {path}") from exc

    if row_count == 0:
        raise ValidationError("Taxi Zone CSV contains no data rows.")

    result = TaxiZonesValidationResult(
        row_count=row_count,
        columns=columns,
        unique_location_count=len(location_ids),
    )
    logger.info(
        "Validated Taxi Zone CSV: rows=%s unique_locations=%s",
        result.row_count,
        result.unique_location_count,
    )
    return result

