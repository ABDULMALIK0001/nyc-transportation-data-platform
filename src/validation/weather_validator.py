"""Validate raw hourly weather JSON before Bronze storage."""

from dataclasses import dataclass
import json
from pathlib import Path

from src.common.exceptions import ValidationError
from src.common.logger import get_logger
from src.ingestion.weather_extractor import HOURLY_VARIABLES


logger = get_logger(__name__)


@dataclass(frozen=True)
class WeatherValidationResult:
    """Profile of a valid hourly weather response."""

    row_count: int
    timezone: str
    variables: tuple[str, ...]


def validate_weather_file(path: Path) -> WeatherValidationResult:
    """Require hourly timestamps and equal-length arrays for every variable."""
    if path.suffix.lower() != ".json":
        raise ValidationError(f"Expected a JSON file, got: {path.name}")
    if not path.exists() or path.stat().st_size == 0:
        raise ValidationError(f"File is missing or empty: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError(f"Unreadable weather JSON: {path}") from exc

    if payload.get("error"):
        raise ValidationError(f"Weather API error: {payload.get('reason', 'unknown')}")
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        raise ValidationError("Weather JSON is missing the hourly object.")

    timestamps = hourly.get("time")
    if not isinstance(timestamps, list) or not timestamps:
        raise ValidationError("Weather JSON contains no hourly timestamps.")
    if len(set(timestamps)) != len(timestamps):
        raise ValidationError("Weather JSON contains duplicate hourly timestamps.")

    for variable in HOURLY_VARIABLES:
        values = hourly.get(variable)
        if not isinstance(values, list):
            raise ValidationError(f"Missing hourly weather variable: {variable}")
        if len(values) != len(timestamps):
            raise ValidationError(
                f"Length mismatch for {variable}: "
                f"expected {len(timestamps)}, got {len(values)}."
            )

    timezone = payload.get("timezone")
    if not isinstance(timezone, str) or not timezone:
        raise ValidationError("Weather JSON is missing timezone metadata.")

    result = WeatherValidationResult(
        row_count=len(timestamps),
        timezone=timezone,
        variables=HOURLY_VARIABLES,
    )
    logger.info(
        "Validated weather JSON: hourly_rows=%s timezone=%s",
        result.row_count,
        result.timezone,
    )
    return result

