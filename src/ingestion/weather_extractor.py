"""Extract hourly historical weather for New York City."""

import calendar
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
from pathlib import Path

import requests

from src.common.exceptions import ExtractionError
from src.common.logger import get_logger
from src.config import Settings


logger = get_logger(__name__)
HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
)


@dataclass(frozen=True)
class WeatherDownloadResult:
    """Facts collected while downloading one month of weather JSON."""

    source_url: str
    local_path: Path
    file_name: str
    size_bytes: int
    sha256: str
    downloaded_at: datetime
    reused_local_file: bool


class WeatherExtractor:
    """Call the Open-Meteo archive endpoint for one calendar month."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_params(self, year: int, month: int) -> dict[str, str | float]:
        self._validate_period(year, month)
        final_day = calendar.monthrange(year, month)[1]
        return {
            "latitude": self.settings.nyc_latitude,
            "longitude": self.settings.nyc_longitude,
            "start_date": f"{year}-{month:02d}-01",
            "end_date": f"{year}-{month:02d}-{final_day:02d}",
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": "America/New_York",
        }

    def build_source_url(self, year: int, month: int) -> str:
        """Return the complete auditable API URL, including query parameters."""
        request = requests.Request(
            "GET",
            self.settings.weather_archive_url,
            params=self.build_params(year, month),
        ).prepare()
        if request.url is None:
            raise ExtractionError("Could not construct the weather source URL.")
        return request.url

    def download(
        self, year: int, month: int, force: bool = False
    ) -> WeatherDownloadResult:
        params = self.build_params(year, month)
        source_url = self.build_source_url(year, month)
        file_name = f"weather_{year}-{month:02d}.json"
        self.settings.download_dir.mkdir(parents=True, exist_ok=True)
        destination = self.settings.download_dir / file_name

        if destination.exists() and destination.stat().st_size > 0 and not force:
            logger.info("Reusing local weather file: %s", destination)
            return self._result_for_file(
                source_url,
                destination,
                reused=True,
            )

        logger.info("Requesting historical weather for %s-%02d", year, month)
        try:
            response = requests.get(
                self.settings.weather_archive_url,
                params=params,
                timeout=(10, 120),
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("error"):
                raise ExtractionError(
                    f"Weather API rejected the request: {payload.get('reason')}"
                )
            raw_content = response.content
            if not raw_content:
                raise ExtractionError("Weather API returned an empty response.")
            destination.write_bytes(raw_content)
        except (requests.RequestException, OSError, ValueError) as exc:
            raise ExtractionError(f"Could not extract weather data: {exc}") from exc

        digest = hashlib.sha256(raw_content).hexdigest()
        logger.info("Downloaded %s bytes to %s", len(raw_content), destination)
        return WeatherDownloadResult(
            source_url=response.url,
            local_path=destination,
            file_name=file_name,
            size_bytes=len(raw_content),
            sha256=digest,
            downloaded_at=datetime.now(UTC),
            reused_local_file=False,
        )

    @staticmethod
    def _validate_period(year: int, month: int) -> None:
        if year < 1940 or year > datetime.now(UTC).year:
            raise ExtractionError("Weather year must be between 1940 and the current year.")
        if month < 1 or month > 12:
            raise ExtractionError("month must be between 1 and 12.")

    @staticmethod
    def _result_for_file(
        source_url: str, path: Path, reused: bool
    ) -> WeatherDownloadResult:
        raw_content = path.read_bytes()
        return WeatherDownloadResult(
            source_url=source_url,
            local_path=path,
            file_name=path.name,
            size_bytes=len(raw_content),
            sha256=hashlib.sha256(raw_content).hexdigest(),
            downloaded_at=datetime.now(UTC),
            reused_local_file=reused,
        )
