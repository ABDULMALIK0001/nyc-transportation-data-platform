"""Download the official NYC TLC Taxi Zone lookup table."""

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
class ReferenceDownloadResult:
    """Facts collected while downloading one reference file."""

    source_url: str
    local_path: Path
    file_name: str
    size_bytes: int
    sha256: str
    downloaded_at: datetime
    reused_local_file: bool


class TaxiZonesExtractor:
    """Extract the small, non-periodic Taxi Zone CSV lookup."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def download(self, force: bool = False) -> ReferenceDownloadResult:
        source_url = self.settings.taxi_zone_lookup_url
        file_name = "taxi_zone_lookup.csv"
        self.settings.download_dir.mkdir(parents=True, exist_ok=True)
        destination = self.settings.download_dir / file_name

        if destination.exists() and destination.stat().st_size > 0 and not force:
            logger.info("Reusing local reference file: %s", destination)
            return self._result_for_file(source_url, destination, reused=True)

        partial_path = destination.with_suffix(".csv.part")
        digest = hashlib.sha256()
        size_bytes = 0
        logger.info("Downloading %s", source_url)
        try:
            with requests.get(source_url, stream=True, timeout=(10, 60)) as response:
                response.raise_for_status()
                with partial_path.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=64 * 1024):
                        if not chunk:
                            continue
                        output.write(chunk)
                        digest.update(chunk)
                        size_bytes += len(chunk)
            if size_bytes == 0:
                raise ExtractionError("The Taxi Zone source returned an empty file.")
            partial_path.replace(destination)
        except (requests.RequestException, OSError) as exc:
            partial_path.unlink(missing_ok=True)
            raise ExtractionError(f"Could not download {source_url}: {exc}") from exc

        logger.info("Downloaded %s bytes to %s", size_bytes, destination)
        return ReferenceDownloadResult(
            source_url=source_url,
            local_path=destination,
            file_name=file_name,
            size_bytes=size_bytes,
            sha256=digest.hexdigest(),
            downloaded_at=datetime.now(UTC),
            reused_local_file=False,
        )

    @staticmethod
    def _result_for_file(
        source_url: str, path: Path, reused: bool
    ) -> ReferenceDownloadResult:
        digest = hashlib.sha256()
        with path.open("rb") as input_file:
            for chunk in iter(lambda: input_file.read(64 * 1024), b""):
                digest.update(chunk)
        return ReferenceDownloadResult(
            source_url=source_url,
            local_path=path,
            file_name=path.name,
            size_bytes=path.stat().st_size,
            sha256=digest.hexdigest(),
            downloaded_at=datetime.now(UTC),
            reused_local_file=reused,
        )

