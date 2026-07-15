"""Store Bronze source files and Silver output files in MinIO."""

from dataclasses import dataclass
import hashlib
from pathlib import Path

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError, EndpointConnectionError

from src.common.exceptions import StorageError
from src.common.logger import get_logger
from src.config import Settings


logger = get_logger(__name__)


@dataclass(frozen=True)
class UploadResult:
    """Result of an idempotent Bronze upload."""

    bucket: str
    object_key: str
    skipped: bool


@dataclass(frozen=True)
class DirectoryUploadResult:
    """Summary of publishing a local Silver output directory."""

    bucket: str
    object_prefix: str
    uploaded_files: int
    skipped_files: int
    deleted_files: int


class MinioStorage:
    """Small S3-compatible client focused on Bronze ingestion."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client: BaseClient = boto3.client(
            "s3",
            endpoint_url=f"http://{settings.minio_endpoint}",
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
            region_name="us-east-1",
        )

    def upload_bronze_file(
        self,
        local_path: Path,
        object_key: str,
        sha256: str,
        source_url: str,
        row_count: int,
        content_type: str = "application/vnd.apache.parquet",
    ) -> UploadResult:
        """Upload once; skip when the same checksum is already stored."""
        bucket = self.settings.minio_bronze_bucket
        try:
            self._ensure_bucket(bucket)
            existing_checksum = self._existing_checksum(bucket, object_key)
            if existing_checksum == sha256:
                logger.info("Bronze object already exists with same checksum; skipping upload.")
                return UploadResult(bucket=bucket, object_key=object_key, skipped=True)

            self.client.upload_file(
                str(local_path),
                bucket,
                object_key,
                ExtraArgs={
                    "ContentType": content_type,
                    "Metadata": {
                        "sha256": sha256,
                        "source-url": source_url,
                        "row-count": str(row_count),
                    },
                },
            )
        except (ClientError, EndpointConnectionError, OSError) as exc:
            raise StorageError(f"Could not upload {local_path} to MinIO: {exc}") from exc

        logger.info("Uploaded Bronze object: s3://%s/%s", bucket, object_key)
        return UploadResult(bucket=bucket, object_key=object_key, skipped=False)

    def upload_silver_directory(
        self, local_directory: Path, object_prefix: str
    ) -> DirectoryUploadResult:
        """Publish deterministic Parquet and JSON outputs to the Silver bucket."""
        if not local_directory.exists():
            raise StorageError(f"Silver output directory does not exist: {local_directory}")

        bucket = self.settings.minio_silver_bucket
        uploaded_files = 0
        skipped_files = 0
        deleted_files = 0
        try:
            self._ensure_bucket(bucket)
            files = sorted(
                path
                for path in local_directory.rglob("*")
                if path.is_file() and path.suffix.lower() in {".parquet", ".json"}
            )
            if not files:
                raise StorageError(
                    f"Silver output contains no publishable files: {local_directory}"
                )

            desired_keys: set[str] = set()
            for path in files:
                relative_key = path.relative_to(local_directory).as_posix()
                object_key = f"{object_prefix.rstrip('/')}/{relative_key}"
                desired_keys.add(object_key)
                checksum = self._sha256(path)
                if self._existing_checksum(bucket, object_key) == checksum:
                    skipped_files += 1
                    continue
                content_type = (
                    "application/json"
                    if path.suffix.lower() == ".json"
                    else "application/vnd.apache.parquet"
                )
                self.client.upload_file(
                    str(path),
                    bucket,
                    object_key,
                    ExtraArgs={
                        "ContentType": content_type,
                        "Metadata": {"sha256": checksum, "data-layer": "silver"},
                    },
                )
                uploaded_files += 1

            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=bucket, Prefix=f"{object_prefix.rstrip('/')}/"
            ):
                for stored_object in page.get("Contents", []):
                    stored_key = stored_object["Key"]
                    if stored_key not in desired_keys:
                        self.client.delete_object(Bucket=bucket, Key=stored_key)
                        deleted_files += 1
        except (ClientError, EndpointConnectionError, OSError) as exc:
            raise StorageError(
                f"Could not publish Silver directory {local_directory}: {exc}"
            ) from exc

        logger.info(
            "Published Silver prefix s3://%s/%s: uploaded=%s skipped=%s deleted=%s",
            bucket,
            object_prefix,
            uploaded_files,
            skipped_files,
            deleted_files,
        )
        return DirectoryUploadResult(
            bucket=bucket,
            object_prefix=object_prefix,
            uploaded_files=uploaded_files,
            skipped_files=skipped_files,
            deleted_files=deleted_files,
        )

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as input_file:
            for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _ensure_bucket(self, bucket: str) -> None:
        try:
            self.client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            error_code = str(exc.response.get("Error", {}).get("Code", ""))
            if error_code in {"404", "NoSuchBucket", "NotFound"}:
                self.client.create_bucket(Bucket=bucket)
                logger.info("Created MinIO bucket: %s", bucket)
                return
            raise

    def _existing_checksum(self, bucket: str, object_key: str) -> str | None:
        try:
            response = self.client.head_object(Bucket=bucket, Key=object_key)
            return response.get("Metadata", {}).get("sha256")
        except ClientError as exc:
            error_code = str(exc.response.get("Error", {}).get("Code", ""))
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
