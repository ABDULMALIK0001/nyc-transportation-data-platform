"""Unit tests for Silver enrichment metric validation."""

import json

import pytest

from src.common.exceptions import PipelineError
from src.enrichment_pipeline import load_enrichment_metrics


def _metrics() -> dict[str, int]:
    return {
        "zone_rows": 265,
        "weather_rows": 744,
        "input_trip_rows": 100,
        "enriched_trip_rows": 100,
        "missing_pickup_zones": 0,
        "missing_dropoff_zones": 0,
        "missing_weather_hours": 0,
    }


def test_accepts_row_preserving_enrichment(tmp_path) -> None:
    path = tmp_path / "metrics.json"
    path.write_text(json.dumps(_metrics()), encoding="utf-8")

    result = load_enrichment_metrics(path)

    assert result["zone_rows"] == 265


def test_rejects_join_row_count_change(tmp_path) -> None:
    metrics = _metrics()
    metrics["enriched_trip_rows"] = 99
    path = tmp_path / "metrics.json"
    path.write_text(json.dumps(metrics), encoding="utf-8")

    with pytest.raises(PipelineError, match="changed"):
        load_enrichment_metrics(path)

