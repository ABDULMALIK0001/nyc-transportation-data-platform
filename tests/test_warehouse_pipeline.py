"""Unit tests for Warehouse row accounting."""

import pytest

from src.common.exceptions import PipelineError
from src.warehouse.loader import WarehouseLoadResult
from src.warehouse_pipeline import validate_load_result


def test_accepts_complete_warehouse_load() -> None:
    result = WarehouseLoadResult(
        source_rows=100,
        staging_rows=100,
        inserted_rows=80,
        existing_rows=20,
    )

    validate_load_result(result)


def test_rejects_unaccounted_warehouse_rows() -> None:
    result = WarehouseLoadResult(
        source_rows=100,
        staging_rows=100,
        inserted_rows=80,
        existing_rows=10,
    )

    with pytest.raises(PipelineError, match="every source row"):
        validate_load_result(result)


def test_accepts_fast_skip_for_loaded_month() -> None:
    result = WarehouseLoadResult(
        source_rows=100,
        staging_rows=0,
        inserted_rows=0,
        existing_rows=100,
    )

    validate_load_result(result)
