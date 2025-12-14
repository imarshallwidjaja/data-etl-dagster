# =============================================================================
# GDAL Resource - CLI wrapper for spatial data operations
# =============================================================================
# Provides a thin, stateless wrapper around GDAL CLI operations.
# Designed for Dagster integration and future migration to Dagster Pipes.
# =============================================================================

from dataclasses import dataclass
import subprocess
from typing import Dict, Optional
import logging

from dagster import ConfigurableResource
from pydantic import Field

__all__ = ["GDALResource", "GDALResult"]

logger = logging.getLogger(__name__)


@dataclass
class GDALResult:
    """
    Serializable result from GDAL operations.
    
    All fields are JSON-serializable, enabling future Dagster Pipes migration
    where GDAL runs in a separate process/container.
    """
    success: bool
    command: list[str]
    stdout: str
    stderr: str
    return_code: int
    output_path: Optional[str] = None


class GDALResource(ConfigurableResource):
    """
    Dagster resource for GDAL CLI operations.
    
    Provides a thin wrapper around GDAL command-line tools (ogr2ogr, gdal_translate,
    ogrinfo, gdalinfo) for spatial data processing.
    
    Design Principles:
    - **Stateless:** No internal state between calls
    - **Serializable I/O:** All inputs/outputs are JSON-serializable
    - **Pipes-Ready:** Can be migrated to Dagster Pipes (separate process)
    - **S3-Compatible:** Uses /vsis3/ virtual file system for MinIO access
    
    Configuration:
        aws_access_key_id: AWS/MinIO access key for /vsis3/ S3 access
        aws_secret_access_key: AWS/MinIO secret key
        aws_s3_endpoint: MinIO endpoint URL (e.g., "http://minio:9000")
        gdal_data_path: Path to GDAL data files (optional, typically set in container)
        proj_lib_path: Path to PROJ data files (optional, typically set in container)
    
    Example:
        >>> gdal = GDALResource(
        ...     aws_access_key_id="minioadmin",
        ...     aws_secret_access_key="minioadmin",
        ...     aws_s3_endpoint="http://minio:9000"
        ... )
        >>> result = gdal.ogr2ogr(
        ...     input_path="/vsis3/landing-zone/data.geojson",
        ...     output_path="PG:host=postgis dbname=spatial_compute",
        ...     layer_name="raw_input",
        ...     target_crs="EPSG:4326"
        ... )
        >>> if result.success:
        ...     logger.info(f"ogr2ogr succeeded: {result.command}")
        ... else:
        ...     logger.error(f"ogr2ogr failed: {result.stderr}")
    """
    
    aws_access_key_id: str = Field(
        "",
        description="AWS/MinIO access key for /vsis3/ S3 access",
    )
    aws_secret_access_key: str = Field(
        "",
        description="AWS/MinIO secret key for /vsis3/ S3 access",
    )
    aws_s3_endpoint: str = Field(
        "",
        description="MinIO endpoint URL (e.g., http://minio:9000)",
    )
    gdal_data_path: str = Field(
        "",
        description="Path to GDAL data files (optional, defaults set in container)",
    )
    proj_lib_path: str = Field(
        "",
        description="Path to PROJ data files (optional, defaults set in container)",
    )
    
    def _get_env(self) -> Dict[str, str]:
        """
        Build environment variables for GDAL subprocess calls.
        
        Includes S3 credentials for /vsis3/ access and optional GDAL paths.
        
        Returns:
            Dictionary of environment variables to pass to subprocess
        """
        import os
        
        env = os.environ.copy()
        
        # S3 credentials for /vsis3/ virtual file system (MinIO access)
        if self.aws_access_key_id:
            env["AWS_ACCESS_KEY_ID"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            env["AWS_SECRET_ACCESS_KEY"] = self.aws_secret_access_key
        if self.aws_s3_endpoint:
            env["AWS_S3_ENDPOINT"] = self.aws_s3_endpoint
        
        # Optional GDAL/PROJ paths (typically already set in Dockerfile)
        if self.gdal_data_path:
            env["GDAL_DATA"] = self.gdal_data_path
        if self.proj_lib_path:
            env["PROJ_LIB"] = self.proj_lib_path
        
        return env
    
    def ogr2ogr(
        self,
        input_path: str,
        output_path: str,
        output_format: str = "PostgreSQL",
        target_crs: Optional[str] = None,
        layer_name: Optional[str] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> GDALResult:
        """
        Convert vector data using ogr2ogr.
        
        Supports input from various sources (local, /vsis3/, /vsicurl/) and output
        to multiple formats including PostgreSQL, Parquet, GeoJSON, Shapefile, etc.
        
        Args:
            input_path: Source path (local path, /vsis3/, /vsicurl/, etc.)
            output_path: Destination (file path or connection string)
            output_format: GDAL driver name (default: PostgreSQL)
                Examples: PostgreSQL, Parquet, GeoJSON, ESRI Shapefile, GPX
            target_crs: Target coordinate reference system (e.g., "EPSG:4326")
            layer_name: Output layer/table name
            options: Additional GDAL options as key-value pairs
        
        Returns:
            GDALResult with command execution details and success status
        
        Example:
            >>> result = gdal.ogr2ogr(
            ...     input_path="/vsis3/landing-zone/batch_123/data.geojson",
            ...     output_path="PG:host=postgis dbname=spatial_compute",
            ...     layer_name="proc_abc123.raw_input",
            ...     target_crs="EPSG:4326",
            ...     options={"-overwrite": ""}
            ... )
        """
        cmd = ["ogr2ogr", "-f", output_format]
        
        if target_crs:
            cmd.extend(["-t_srs", target_crs])
        if layer_name:
            cmd.extend(["-nln", layer_name])
        
        # Add custom options
        if options:
            for key, value in options.items():
                cmd.append(key)
                if value:  # Only append value if non-empty
                    cmd.append(value)
        
        cmd.extend([output_path, input_path])
        
        # For PostgreSQL output, don't track output_path (it's a connection string)
        track_output = None if output_format == "PostgreSQL" else output_path
        
        return self._run_command(cmd, track_output)
    
    def gdal_translate(
        self,
        input_path: str,
        output_path: str,
        output_format: str = "COG",
        options: Optional[Dict[str, str]] = None,
    ) -> GDALResult:
        """
        Convert raster data using gdal_translate.
        
        Supports input from various sources and output to multiple raster formats.
        
        Args:
            input_path: Source raster (local path, /vsis3/, /vsicurl/, etc.)
            output_path: Destination file path
            output_format: GDAL driver name (default: COG - Cloud Optimized GeoTIFF)
                Examples: COG, GTiff, PNG, JPEG, JP2OpenJPEG
            options: Additional GDAL options as key-value pairs
        
        Returns:
            GDALResult with execution details
        
        Example:
            >>> result = gdal.gdal_translate(
            ...     input_path="/vsis3/landing-zone/batch_123/image.tif",
            ...     output_path="/vsis3/data-lake/processed/image_cog.tif",
            ...     output_format="COG",
            ...     options={"-co": "COMPRESS=deflate"}
            ... )
        """
        cmd = ["gdal_translate", "-of", output_format]
        
        if options:
            for key, value in options.items():
                cmd.append(key)
                if value:
                    cmd.append(value)
        
        cmd.extend([input_path, output_path])
        
        return self._run_command(cmd, output_path)
    
    def ogrinfo(
        self,
        input_path: str,
        layer: Optional[str] = None,
        as_json: bool = False,
    ) -> GDALResult:
        """
        Get information about a vector dataset.

        Displays schema, feature count, coordinate system, and spatial extent.

        Args:
            input_path: Path to vector dataset
            layer: Specific layer to inspect (optional)
            as_json: Return output in JSON format (optional, default: False)

        Returns:
            GDALResult with dataset info in stdout

        Example:
            >>> result = gdal.ogrinfo("/vsis3/landing-zone/data.geojson")
            >>> if result.success:
            ...     print(result.stdout)

            >>> result = gdal.ogrinfo("/vsis3/landing-zone/data.geojson", as_json=True)
            >>> import json
            >>> schema = json.loads(result.stdout)
        """
        cmd = ["ogrinfo"]
        if as_json:
            cmd.append("-json")
        else:
            cmd.extend(["-al", "-so"])
        cmd.append(input_path)
        if layer:
            cmd.append(layer)

        return self._run_command(cmd)
    
    def gdalinfo(self, input_path: str) -> GDALResult:
        """
        Get information about a raster dataset.
        
        Displays metadata, coordinate system, extent, and band information.
        
        Args:
            input_path: Path to raster dataset
        
        Returns:
            GDALResult with dataset info in stdout
        
        Example:
            >>> result = gdal.gdalinfo("/vsis3/landing-zone/image.tif")
            >>> if result.success:
            ...     print(result.stdout)
        """
        cmd = ["gdalinfo", input_path]
        return self._run_command(cmd)
    
    def _run_command(
        self,
        cmd: list[str],
        output_path: Optional[str] = None,
    ) -> GDALResult:
        """
        Execute a GDAL command via subprocess.
        
        Args:
            cmd: Command and arguments as list (e.g., ["ogr2ogr", "-f", "GeoJSON", ...])
            output_path: Optional path to track as output (for success verification)
        
        Returns:
            GDALResult with execution details, stdout, stderr, and return code
        """
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=self._get_env(),
        )
        
        return GDALResult(
            success=result.returncode == 0,
            command=cmd,
            stdout=result.stdout,
            stderr=result.stderr,
            return_code=result.returncode,
            output_path=output_path if result.returncode == 0 else None,
        )

    
    def run_raw_command(self, cmd: list[str]) -> GDALResult:
        """
        Execute an arbitrary GDAL command via subprocess.
        
        This method is useful for health checks, version queries, and format listings.
        
        Args:
            cmd: Command and arguments as list (e.g., ["gdalinfo", "--version"])
        
        Returns:
            GDALResult with execution details, stdout, stderr, and return code
        
        Example:
            >>> result = gdal.run_raw_command(["gdalinfo", "--version"])
            >>> if result.success:
            ...     print(result.stdout)
        """
        return self._run_command(cmd)
