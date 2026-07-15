"""Unit tests for hourly weather JSON validation."""

import json

import pytest

from src.common.exceptions import ValidationError
from src.ingestion.weather_extractor import HOURLY_VARIABLES
from src.validation.weather_validator import validate_weather_file


def _payload() -> dict[str, object]:
    hourly: dict[str, object] = {
        "time": ["2024-08-01T00:00", "2024-08-01T01:00"]
    }
    for variable in HOURLY_VARIABLES:
        hourly[variable] = [1, 2]
    return {"timezone": "America/New_York", "hourly": hourly}


def test_accepts_equal_length_hourly_arrays(tmp_path) -> None:
    path = tmp_path / "weather_2024-08.json"
    path.write_text(json.dumps(_payload()), encoding="utf-8")

    result = validate_weather_file(path)

    assert result.row_count == 2


def test_rejects_hourly_length_mismatch(tmp_path) -> None:
    payload = _payload()
    payload["hourly"]["precipitation"] = [1]
    path = tmp_path / "weather_2024-08.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValidationError, match="Length mismatch"):
        validate_weather_file(path)

