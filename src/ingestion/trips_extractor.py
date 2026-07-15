"""Download monthly NYC TLC trip records from the official public source."""

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
from pathlib import Path

import requests

from src.common.exceptions import ExtractionError
from src.common.logger import get_logger
from src.config import Settings


logger = get_logger(__name__)


@dataclass(frozen=True)
class DownloadResult:
    """Facts collected while downloading one source file."""

    source_url: str
    local_path: Path
    file_name: str
    size_bytes: int
    sha256: str
    downloaded_at: datetime
    reused_local_file: bool


class TripsExtractor:
    """Extract one monthly Parquet file from the NYC TLC dataset."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_url(self, year: int, month: int, taxi_type: str = "yellow") -> str:
        self._validate_period(year, month)
        if taxi_type not in {"yellow", "green"}:
            raise ExtractionError("taxi_type must be either 'yellow' or 'green'.")
        file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        return f"{self.settings.tlc_trip_data_base_url}/{file_name}"

    def download(
        self,
        year: int,
        month: int,
        taxi_type: str = "yellow",
        force: bool = False,
    ) -> DownloadResult:
        """Stream a monthly file to disk and calculate its SHA-256 checksum."""
        source_url = self.build_url(year, month, taxi_type)
        file_name = source_url.rsplit("/", maxsplit=1)[-1]
        self.settings.download_dir.mkdir(parents=True, exist_ok=True)
        destination = self.settings.download_dir / file_name

        if destination.exists() and destination.stat().st_size > 0 and not force:
            logger.info("Reusing local file: %s", destination)
            return self._result_for_existing_file(source_url, destination)

        partial_path = destination.with_suffix(destination.suffix + ".part")
        digest = hashlib.sha256()
        size_bytes = 0
        logger.info("Downloading %s", source_url)

        try:
            with requests.get(source_url, stream=True, timeout=(10, 120)) as response:
                response.raise_for_status()
                expected_size = int(response.headers.get("content-length", 0))
                with partial_path.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        output.write(chunk)
                        digest.update(chunk)
                        size_bytes += len(chunk)

            if size_bytes == 0:
                raise ExtractionError("The source returned an empty file.")
            if expected_size and size_bytes != expected_size:
                raise ExtractionError(
                    f"Incomplete download: expected {expected_size} bytes, got {size_bytes}."
                )
            partial_path.replace(destination)
        except (requests.RequestException, OSError) as exc:
            partial_path.unlink(missing_ok=True)
            raise ExtractionError(f"Could not download {source_url}: {exc}") from exc

        logger.info("Downloaded %s bytes to %s", size_bytes, destination)
        return DownloadResult(
            source_url=source_url,
            local_path=destination,
            file_name=file_name,
            size_bytes=size_bytes,
            sha256=digest.hexdigest(),
            downloaded_at=datetime.now(UTC),
            reused_local_file=False,
        )

    @staticmethod
    def _validate_period(year: int, month: int) -> None:
        if year < 2009 or year > datetime.now(UTC).year:
            raise ExtractionError("year must be between 2009 and the current year.")
        if month < 1 or month > 12:
            raise ExtractionError("month must be between 1 and 12.")

    @staticmethod
    def _result_for_existing_file(source_url: str, path: Path) -> DownloadResult:
        digest = hashlib.sha256()
        with path.open("rb") as input_file:
            for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
                digest.update(chunk)
        return DownloadResult(
            source_url=source_url,
            local_path=path,
            file_name=path.name,
            size_bytes=path.stat().st_size,
            sha256=digest.hexdigest(),
            downloaded_at=datetime.now(UTC),
            reused_local_file=True,
        )

