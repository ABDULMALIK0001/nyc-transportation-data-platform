"""Central project configuration loaded from environment variables."""

from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    """Settings shared by the ingestion, validation, and storage modules."""

    project_root: Path = PROJECT_ROOT
    download_dir: Path = PROJECT_ROOT / "data" / "downloads"
    tlc_trip_data_base_url: str = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data"
    )
    taxi_zone_lookup_url: str = (
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    )
    weather_archive_url: str = "https://archive-api.open-meteo.com/v1/archive"
    nyc_latitude: float = 40.7128
    nyc_longitude: float = -74.0060
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_bronze_bucket: str = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
    minio_silver_bucket: str = os.getenv("MINIO_SILVER_BUCKET", "silver")
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "nyc_data")
    postgres_user: str = os.getenv("POSTGRES_USER", "pipeline_user")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "pipeline_password")

    @property
    def postgres_dsn(self) -> str:
        """Return the connection string used by psycopg."""
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


settings = Settings()
