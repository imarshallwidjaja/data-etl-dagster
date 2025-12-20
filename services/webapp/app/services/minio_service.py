# =============================================================================
# MinIO Service - S3-Compatible Object Storage Operations
# =============================================================================
# Service wrapper for MinIO operations in the webapp.
# =============================================================================

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import BytesIO
from typing import BinaryIO, Optional

from minio import Minio
from minio.error import S3Error

from app.config import get_settings


@dataclass
class LandingZoneObject:
    """Represents an object in the landing zone."""

    key: str
    size: int
    last_modified: datetime
    is_dir: bool = False


# Protected prefixes that cannot be deleted
PROTECTED_PREFIXES = ("archive/", "manifests/")


class MinIOService:
    """Service for MinIO operations."""

    def __init__(self) -> None:
        settings = get_settings()
        self._client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_root_user,
            secret_key=settings.minio_root_password,
            secure=settings.minio_use_ssl,
        )
        self._landing_bucket = settings.minio_landing_bucket
        self._lake_bucket = settings.minio_lake_bucket

    def list_landing_zone(
        self, prefix: str = "", include_archive: bool = False
    ) -> list[LandingZoneObject]:
        """
        List objects in the landing zone.

        Args:
            prefix: Optional prefix to filter objects
            include_archive: Whether to include archive/ prefix

        Returns:
            List of LandingZoneObject
        """
        objects = []

        try:
            # Use recursive=False to only list immediate children
            # This provides proper hierarchical browsing
            items = self._client.list_objects(
                self._landing_bucket,
                prefix=prefix,
                recursive=False,
            )

            for item in items:
                # Skip archive unless explicitly requested
                if not include_archive and item.object_name.startswith("archive/"):
                    continue

                objects.append(
                    LandingZoneObject(
                        key=item.object_name,
                        size=item.size or 0,
                        last_modified=item.last_modified or datetime.now(),
                        is_dir=item.is_dir if hasattr(item, "is_dir") else False,
                    )
                )

        except S3Error as exc:
            if exc.code == "NoSuchBucket":
                return []
            raise

        return objects

    def upload_to_landing(
        self, file: BinaryIO, key: str, content_type: str = "application/octet-stream"
    ) -> None:
        """
        Upload a file to the landing zone.

        Args:
            file: File-like object to upload
            key: Destination object key
            content_type: MIME type of the file
        """
        # Get file size
        file.seek(0, 2)  # Seek to end
        size = file.tell()
        file.seek(0)  # Seek back to start

        self._client.put_object(
            self._landing_bucket,
            key,
            file,
            length=size,
            content_type=content_type,
        )

    def download_from_landing(self, key: str) -> BytesIO:
        """
        Download a file from the landing zone.

        Args:
            key: Object key (supports both active and archive/ prefix)

        Returns:
            BytesIO containing file content
        """
        try:
            response = self._client.get_object(self._landing_bucket, key)
            data = BytesIO(response.read())
            response.close()
            response.release_conn()
            return data
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                raise FileNotFoundError(f"Object not found: {key}") from exc
            raise

    def download_from_lake(self, key: str) -> BytesIO:
        """
        Download a file from the data lake.

        Args:
            key: Object key in data lake bucket

        Returns:
            BytesIO containing file content
        """
        try:
            response = self._client.get_object(self._lake_bucket, key)
            data = BytesIO(response.read())
            response.close()
            response.release_conn()
            return data
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                raise FileNotFoundError(f"Object not found: {key}") from exc
            raise

    def delete_from_landing(self, key: str) -> None:
        """
        Delete a file from the landing zone.

        Args:
            key: Object key to delete

        Note:
            Only allows deletion from active landing zone, not archive.
        """
        if key.startswith("archive/"):
            raise PermissionError("Cannot delete from archive")

        try:
            self._client.remove_object(self._landing_bucket, key)
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                raise FileNotFoundError(f"Object not found: {key}") from exc
            raise

    def create_folder(self, prefix: str) -> str:
        """
        Create a folder (prefix) in the landing zone.

        In S3/MinIO, folders are represented as zero-byte objects ending with '/'.

        Args:
            prefix: Folder path to create (e.g., "my_folder/" or "parent/child/")

        Returns:
            The created folder key
        """
        # Ensure prefix ends with /
        if not prefix.endswith("/"):
            prefix = f"{prefix}/"

        # Create empty object to represent folder
        self._client.put_object(
            self._landing_bucket,
            prefix,
            data=BytesIO(b""),
            length=0,
        )
        return prefix

    def folder_is_empty(self, prefix: str) -> bool:
        """
        Check if a folder is empty.

        Args:
            prefix: Folder path to check

        Returns:
            True if folder has no children (besides the folder marker itself)
        """
        # Ensure prefix ends with /
        if not prefix.endswith("/"):
            prefix = f"{prefix}/"

        # List objects in the folder
        items = list(
            self._client.list_objects(
                self._landing_bucket,
                prefix=prefix,
                recursive=False,
            )
        )

        # A folder is empty if it only contains itself (the folder marker)
        # or has no items
        if len(items) == 0:
            return True
        if len(items) == 1 and items[0].object_name == prefix:
            return True
        return False

    def delete_folder(self, prefix: str) -> None:
        """
        Delete an empty folder from the landing zone.

        Args:
            prefix: Folder path to delete

        Raises:
            PermissionError: If folder is protected (archive/, manifests/)
            ValueError: If folder is not empty
            FileNotFoundError: If folder doesn't exist
        """
        # Ensure prefix ends with /
        if not prefix.endswith("/"):
            prefix = f"{prefix}/"

        # Check for protected prefixes
        for protected in PROTECTED_PREFIXES:
            if prefix == protected or prefix.startswith(protected):
                raise PermissionError(f"Cannot delete protected folder: {prefix}")

        # Check if folder is empty
        if not self.folder_is_empty(prefix):
            raise ValueError(f"Cannot delete folder '{prefix}': folder is not empty")

        # Delete the folder marker
        try:
            self._client.remove_object(self._landing_bucket, prefix)
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                raise FileNotFoundError(f"Folder not found: {prefix}") from exc
            raise

    def get_archived_manifest(self, key: str) -> dict:
        """
        Get and parse a manifest from the archive.

        Args:
            key: Manifest key (with or without archive/ prefix)

        Returns:
            Parsed manifest dictionary
        """
        # Ensure archive prefix
        if not key.startswith("archive/"):
            key = f"archive/{key}"

        try:
            data = self.download_from_landing(key)
            return json.loads(data.read().decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in manifest: {key}") from exc

    def generate_download_url(
        self, bucket: str, key: str, expiry_seconds: int = 3600
    ) -> str:
        """
        Generate a presigned download URL.

        Args:
            bucket: Bucket name ("landing" or "lake")
            key: Object key
            expiry_seconds: URL validity duration

        Returns:
            Presigned URL string
        """
        bucket_name = self._landing_bucket if bucket == "landing" else self._lake_bucket

        return self._client.presigned_get_object(
            bucket_name,
            key,
            expires=timedelta(seconds=expiry_seconds),
        )

    def object_exists(self, bucket: str, key: str) -> bool:
        """
        Check if an object exists.

        Args:
            bucket: Bucket name ("landing" or "lake")
            key: Object key

        Returns:
            True if object exists
        """
        bucket_name = self._landing_bucket if bucket == "landing" else self._lake_bucket

        try:
            self._client.stat_object(bucket_name, key)
            return True
        except S3Error:
            return False

    def upload_if_not_exists(
        self, file: BinaryIO, key: str, content_type: str = "application/json"
    ) -> None:
        """
        Upload a file to the landing zone only if the key does not already exist.

        This helps prevent race conditions where concurrent re-run requests
        could overwrite each other.

        Args:
            file: File-like object to upload
            key: Destination object key
            content_type: MIME type of the file

        Raises:
            FileExistsError: If the object already exists
        """
        if self.object_exists("landing", key):
            raise FileExistsError(f"Object already exists: {key}")
        self.upload_to_landing(file, key, content_type)


# Singleton instance
_minio_service: Optional[MinIOService] = None


def get_minio_service() -> MinIOService:
    """Get or create the MinIO service singleton."""
    global _minio_service
    if _minio_service is None:
        _minio_service = MinIOService()
    return _minio_service
