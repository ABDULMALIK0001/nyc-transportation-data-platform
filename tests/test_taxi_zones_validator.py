"""Unit tests for the Taxi Zone reference data contract."""

import pytest

from src.common.exceptions import ValidationError
from src.validation.taxi_zones_validator import validate_taxi_zones_file


def test_accepts_valid_taxi_zones_csv(tmp_path) -> None:
    path = tmp_path / "taxi_zone_lookup.csv"
    path.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "1,EWR,Newark Airport,EWR\n"
        "2,Queens,Jamaica Bay,Boro Zone\n",
        encoding="utf-8",
    )

    result = validate_taxi_zones_file(path)

    assert result.row_count == 2
    assert result.unique_location_count == 2


def test_rejects_duplicate_location_id(tmp_path) -> None:
    path = tmp_path / "taxi_zone_lookup.csv"
    path.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "1,EWR,Newark Airport,EWR\n"
        "1,Queens,Jamaica Bay,Boro Zone\n",
        encoding="utf-8",
    )

    with pytest.raises(ValidationError, match="Duplicate LocationID"):
        validate_taxi_zones_file(path)

