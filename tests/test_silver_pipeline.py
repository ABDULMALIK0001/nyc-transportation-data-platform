"""Unit tests for Silver metrics validation."""

import json

import pytest

from src.common.exceptions import PipelineError
from src.silver_pipeline import load_metrics


def test_accepts_balanced_silver_metrics(tmp_path) -> None:
    path = tmp_path / "metrics.json"
    path.write_text(
        json.dumps({"input_rows": 10, "accepted_rows": 8, "rejected_rows": 2}),
        encoding="utf-8",
    )

    metrics = load_metrics(path)

    assert metrics["accepted_rows"] == 8


def test_rejects_unbalanced_silver_metrics(tmp_path) -> None:
    path = tmp_path / "metrics.json"
    path.write_text(
        json.dumps({"input_rows": 10, "accepted_rows": 8, "rejected_rows": 1}),
        encoding="utf-8",
    )

    with pytest.raises(PipelineError, match="every input row"):
        load_metrics(path)

