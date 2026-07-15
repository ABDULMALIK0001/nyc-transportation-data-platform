"""Unit tests for the Parquet data contract."""

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.common.exceptions import ValidationError
from src.validation.file_validator import validate_yellow_trip_file


def _valid_table() -> pa.Table:
    return pa.table(
        {
            "VendorID": [1],
            "tpep_pickup_datetime": [datetime(2024, 8, 1, 10, 0)],
            "tpep_dropoff_datetime": [datetime(2024, 8, 1, 10, 15)],
            "passenger_count": [1],
            "trip_distance": [2.5],
            "PULocationID": [161],
            "DOLocationID": [236],
            "payment_type": [1],
            "fare_amount": [15.0],
            "total_amount": [19.5],
        }
    )


def test_accepts_valid_yellow_trip_schema(tmp_path) -> None:
    path = tmp_path / "yellow_tripdata_2024-08.parquet"
    pq.write_table(_valid_table(), path)

    result = validate_yellow_trip_file(path)

    assert result.row_count == 1
    assert result.column_count == 10


def test_rejects_missing_required_column(tmp_path) -> None:
    path = tmp_path / "invalid.parquet"
    table = _valid_table().drop(["total_amount"])
    pq.write_table(table, path)

    with pytest.raises(ValidationError, match="total_amount"):
        validate_yellow_trip_file(path)

