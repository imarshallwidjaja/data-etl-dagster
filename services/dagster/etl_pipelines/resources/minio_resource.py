# =============================================================================
# MinIO Resource - S3-Compatible Object Storage Operations
# =============================================================================
# Provides MinIO operations for landing zone and data lake buckets.
# Used by sensors to list manifests, ops to download/upload data.
# =============================================================================

from typing import Optional
import json
import io
from pathlib import Path

from dagster import ConfigurableResource
from minio import Minio
from minio.error import S3Error
from pydantic import Field


class MinIOResource(ConfigurableResource):
    """
    Dagster resource for MinIO (S3-compatible object storage) operations.
    
    Provides methods for:
    - Listing and retrieving manifest files from landing zone
    - Moving processed manifests to archive
    - Uploading processed data to data lake
    - Generating presigned URLs for GDAL /vsicurl/ access
    
    Configuration matches MinIOSettings from libs.models.config.
    
    Attributes:
        endpoint: MinIO server endpoint (host:port)
        access_key: Access key for authentication
        secret_key: Secret key for authentication
        use_ssl: Whether to use SSL/TLS (default: False)
        landing_bucket: Landing zone bucket name (default: "landing-zone")
        lake_bucket: Data lake bucket name (default: "data-lake")
    """
    
    endpoint: str = Field(..., description="MinIO server endpoint (host:port)")
    access_key: str = Field(..., description="Access key for authentication")
    secret_key: str = Field(..., description="Secret key for authentication")
    use_ssl: bool = Field(False, description="Whether to use SSL/TLS")
    landing_bucket: str = Field("landing-zone", description="Landing zone bucket name")
    lake_bucket: str = Field("data-lake", description="Data lake bucket name")
    
    def get_client(self) -> Minio:
        """
        Create a MinIO client instance.
        
        Returns:
            Configured Minio client
        """
        return Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.use_ssl,
        )
    
    def list_manifests(self) -> list[str]:
        """
        List all manifest JSON files in the landing zone.
        
        Scans the `manifests/` prefix in the landing bucket and returns
        keys for all `.json` files.
        
        Returns:
            List of manifest object keys (e.g., ["manifests/batch_123.json"])
            Empty list if no manifests found.
            
        Raises:
            S3Error: If bucket doesn't exist or access denied
        """
        client = self.get_client()
        prefix = "manifests/"
        
        try:
            objects = client.list_objects(
                self.landing_bucket,
                prefix=prefix,
                recursive=True,
            )
            
            # Filter to JSON files only
            manifests = [
                obj.object_name
                for obj in objects
                if obj.object_name.endswith(".json")
            ]
            
            return manifests
            
        except S3Error as exc:
            if exc.code == "NoSuchBucket":
                raise RuntimeError(
                    f"Landing bucket '{self.landing_bucket}' does not exist"
                ) from exc
            raise
    
    def get_manifest(self, key: str) -> dict:
        """
        Download and parse a manifest JSON file.
        
        Args:
            key: Object key for the manifest (e.g., "manifests/batch_123.json")
            
        Returns:
            Parsed manifest as dictionary
            
        Raises:
            S3Error: If object doesn't exist or access denied
            json.JSONDecodeError: If manifest is not valid JSON
            RuntimeError: For other download/parse errors
        """
        client = self.get_client()
        
        try:
            response = client.get_object(self.landing_bucket, key)
            data = response.read()
            response.close()
            response.release_conn()
            
            # Parse JSON
            manifest = json.loads(data)
            return manifest
            
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                raise RuntimeError(
                    f"Manifest '{key}' not found in bucket '{self.landing_bucket}'"
                ) from exc
            raise
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Manifest '{key}' contains invalid JSON: {exc}"
            ) from exc
    
    def download_from_landing(self, s3_key: str, local_path: str) -> None:
        """
        Download a file from the landing zone to a local path.
        
        Args:
            s3_key: Object key in landing bucket (e.g., "batch_001/data.csv")
            local_path: Local file path to write to
            
        Raises:
            S3Error: If object doesn't exist or access denied
            RuntimeError: For other download errors
        """
        from pathlib import Path
        
        client = self.get_client()
        
        try:
            # Ensure parent directory exists
            local_file = Path(local_path)
            local_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Download object
            response = client.get_object(self.landing_bucket, s3_key)
            
            # Write to local file
            with open(local_path, "wb") as f:
                for chunk in response.stream(32 * 1024):  # 32KB chunks
                    f.write(chunk)
            
            response.close()
            response.release_conn()
            
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                raise RuntimeError(
                    f"Object '{s3_key}' not found in bucket '{self.landing_bucket}'"
                ) from exc
            raise
    
    def move_to_archive(self, key: str) -> None:
        """
        Move a processed manifest to the archive folder.
        
        Copies the object from its current location to `archive/{original_path}`
        and then deletes the original. Operation is idempotent - tolerates
        missing source on delete (assumes already moved).
        
        Args:
            key: Object key for the manifest (e.g., "manifests/batch_123.json")
            
        Raises:
            S3Error: If copy fails or source doesn't exist
        """
        client = self.get_client()
        
        # Build archive path: manifests/batch.json -> archive/manifests/batch.json
        archive_key = f"archive/{key}"
        
        # Copy to archive
        client.copy_object(
            self.landing_bucket,
            archive_key,
            f"{self.landing_bucket}/{key}",
        )
        
        # Delete original (tolerate NotFound - already moved)
        try:
            client.remove_object(self.landing_bucket, key)
        except S3Error as exc:
            if exc.code != "NoSuchKey":
                raise
    
    def upload_to_lake(self, local_path: str, s3_key: str, content_type: Optional[str] = None) -> None:
        """
        Upload a processed file to the data lake.
        
        Args:
            local_path: Path to local file to upload
            s3_key: Destination object key in data lake bucket
            content_type: MIME type (default: inferred from extension or 'application/octet-stream')
            
        Raises:
            FileNotFoundError: If local_path doesn't exist
            S3Error: If upload fails
        """
        client = self.get_client()
        
        # Validate local file exists
        path = Path(local_path)
        if not path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")
        
        # Infer content type if not provided
        if content_type is None:
            content_type = self._infer_content_type(path.suffix)
        
        # Get file size
        file_size = path.stat().st_size
        
        # Upload with file stream
        with open(path, "rb") as file_data:
            client.put_object(
                self.lake_bucket,
                s3_key,
                file_data,
                length=file_size,
                content_type=content_type,
            )
    
    def get_presigned_url(self, bucket: str, key: str, expiry_seconds: int = 3600) -> str:
        """
        Generate a presigned GET URL for temporary access.
        
        Used to provide GDAL with /vsicurl/ URLs for reading S3 objects
        without exposing credentials in command-line arguments.
        
        Args:
            bucket: Bucket name (landing_bucket or lake_bucket)
            key: Object key
            expiry_seconds: URL validity duration (default: 3600 = 1 hour)
            
        Returns:
            Presigned URL string
            
        Raises:
            S3Error: If object doesn't exist or presigning fails
        """
        client = self.get_client()
        
        url = client.presigned_get_object(
            bucket,
            key,
            expires=expiry_seconds,
        )
        
        return url
    
    @staticmethod
    def _infer_content_type(suffix: str) -> str:
        """
        Infer MIME type from file extension.
        
        Args:
            suffix: File extension (e.g., ".parquet", ".tif")
            
        Returns:
            MIME type string
        """
        content_types = {
            ".parquet": "application/vnd.apache.parquet",
            ".tif": "image/tiff",
            ".tiff": "image/tiff",
            ".json": "application/json",
            ".geojson": "application/geo+json",
            ".gpkg": "application/geopackage+sqlite3",
        }
        
        return content_types.get(suffix.lower(), "application/octet-stream")

