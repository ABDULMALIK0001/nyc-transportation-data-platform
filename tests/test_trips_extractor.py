"""Unit tests for TLC source URL construction."""

import pytest

from src.common.exceptions import ExtractionError
from src.config import settings
from src.ingestion.trips_extractor import TripsExtractor


def test_builds_partition_source_url() -> None:
    extractor = TripsExtractor(settings)

    url = extractor.build_url(2024, 8)

    assert url.endswith("/yellow_tripdata_2024-08.parquet")


@pytest.mark.parametrize("month", [0, 13])
def test_rejects_invalid_month(month: int) -> None:
    extractor = TripsExtractor(settings)

    with pytest.raises(ExtractionError):
        extractor.build_url(2024, month)

