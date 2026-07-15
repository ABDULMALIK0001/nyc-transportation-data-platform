"""Unit tests for historical weather request construction."""

from src.config import settings
from src.ingestion.weather_extractor import WeatherExtractor


def test_builds_full_month_weather_request() -> None:
    params = WeatherExtractor(settings).build_params(2024, 2)

    assert params["start_date"] == "2024-02-01"
    assert params["end_date"] == "2024-02-29"
    assert params["timezone"] == "America/New_York"

