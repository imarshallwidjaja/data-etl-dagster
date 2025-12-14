# =============================================================================
# Export Op - PostGIS to Data Lake
# =============================================================================
# Exports processed data from PostGIS to MinIO data lake as GeoParquet
# and registers in MongoDB.
# =============================================================================

import hashlib
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

from dagster import op, OpExecutionContext, In, Out

from libs.models import Asset, AssetMetadata, Bounds, OutputFormat, CRS
from libs.spatial_utils import RunIdSchemaMapping
from services.dagster.etl_pipelines.resources.gdal_resource import GDALResult


def _export_to_datalake(
    gdal,
    postgis,
    minio,
    mongodb,
    transform_result: Dict[str, Any],
    run_id: str,
    log,
) -> Dict[str, Any]:
    """
    Core logic for exporting to data lake and registering in MongoDB.
    
    This function is extracted for easier unit testing without Dagster context.
    
    Args:
        gdal: GDALResource instance
        postgis: PostGISResource instance (needed for connection string)
        minio: MinIOResource instance
        mongodb: MongoDBResource instance
        transform_result: Transform result dict from spatial_transform op
        run_id: Dagster run ID
        log: Logger instance (context.log)
    
    Returns:
        Asset info dict with asset_id, s3_key, dataset_id, version, content_hash, run_id
    
    Raises:
        RuntimeError: If GDAL export, hash calculation, upload, or MongoDB insert fails
    """
    schema = transform_result["schema"]
    table = transform_result["table"]
    manifest = transform_result["manifest"]
    bounds_dict = transform_result["bounds"]
    crs = transform_result["crs"]
    
    # Generate dataset_id
    dataset_id = f"dataset_{uuid.uuid4().hex[:12]}"
    log.info(f"Generated dataset_id: {dataset_id}")
    
    # Get next version number
    version = mongodb.get_next_version(dataset_id)
    log.info(f"Dataset version: {version}")
    
    # Generate S3 key
    s3_key = f"{dataset_id}/v{version}/data.parquet"
    log.info(f"Target S3 key: {s3_key}")
    
    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    temp_file_path = temp_file.name
    temp_file.close()
    
    try:
        # Build PostgreSQL connection string for ogr2ogr input
        # Format: PG:host={host} dbname={database} schemas={schema} user={user} password={password} tables={table}
        pg_conn_str = (
            f"PG:host={postgis.host} "
            f"dbname={postgis.database} "
            f"schemas={schema} "
            f"user={postgis.user} "
            f"password={postgis.password} "
            f"tables={table}"
        )
        
        log.info(f"Exporting from PostGIS: {schema}.{table} -> {temp_file_path}")
        
        # Execute ogr2ogr to export PostGIS -> GeoParquet
        result: GDALResult = gdal.ogr2ogr(
            input_path=pg_conn_str,
            output_path=temp_file_path,
            output_format="Parquet",
            target_crs=crs,
        )
        
        # Check for failure
        if not result.success:
            raise RuntimeError(
                f"ogr2ogr export failed: {result.stderr}"
            )
        
        log.info(f"Successfully exported to temporary file: {temp_file_path}")
        
        # Calculate SHA256 content hash
        sha256_hash = hashlib.sha256()
        with open(temp_file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        content_hash = f"sha256:{sha256_hash.hexdigest()}"
        log.info(f"Calculated content hash: {content_hash[:20]}...")
        
        # Upload to MinIO data lake
        minio.upload_to_lake(temp_file_path, s3_key)
        log.info(f"Uploaded to MinIO data lake: {s3_key}")
        
        # Create Asset model
        asset_metadata = AssetMetadata(
            title=manifest["metadata"].get("project", dataset_id),
            description=manifest["metadata"].get("description"),
            source=None,
            license=None,
        )

        # Handle optional bounds
        bounds = None
        if bounds_dict is not None:
            bounds = Bounds(
                minx=bounds_dict["minx"],
                miny=bounds_dict["miny"],
                maxx=bounds_dict["maxx"],
                maxy=bounds_dict["maxy"],
            )

        asset = Asset(
            s3_key=s3_key,
            dataset_id=dataset_id,
            version=version,
            content_hash=content_hash,
            dagster_run_id=run_id,
            format=OutputFormat.GEOPARQUET,
            crs=CRS(crs),
            bounds=bounds,
            metadata=asset_metadata,
            created_at=datetime.now(timezone.utc),
            updated_at=None,
        )
        
        # Register in MongoDB
        inserted_id = mongodb.insert_asset(asset)
        log.info(f"Registered asset in MongoDB: {inserted_id}")
        
        # Return asset info
        return {
            "asset_id": inserted_id,
            "s3_key": s3_key,
            "dataset_id": dataset_id,
            "version": version,
            "content_hash": content_hash,
            "run_id": run_id,
        }
        
    finally:
        # Clean up temporary file
        try:
            Path(temp_file_path).unlink(missing_ok=True)
            log.info(f"Cleaned up temporary file: {temp_file_path}")
        except Exception as e:
            log.warning(f"Failed to clean up temporary file {temp_file_path}: {e}")


@op(
    ins={"transform_result": In(dagster_type=dict)},
    out={"asset_info": Out(dagster_type=dict)},
    required_resource_keys={"gdal", "postgis", "minio", "mongodb"},
)
def export_to_datalake(context: OpExecutionContext, transform_result: dict) -> dict:
    """
    Export processed data from PostGIS to MinIO data lake and register in MongoDB.
    
    Exports data from PostGIS to GeoParquet format, uploads to MinIO data lake,
    calculates content hash, and registers the asset in MongoDB.
    
    After export completes (success or failure), automatically cleans up the
    ephemeral PostGIS schema to maintain architectural law that PostGIS is
    transient compute only.
    
    Args:
        context: Dagster op execution context
        transform_result: Transform result dict from spatial_transform op
    
    Returns:
        Asset info dict containing:
        - asset_id: MongoDB ObjectId as string
        - s3_key: S3 object key in data lake
        - dataset_id: Dataset identifier
        - version: Asset version number
        - content_hash: SHA256 content hash
        - run_id: Dagster run ID
    
    Raises:
        RuntimeError: If GDAL export, hash calculation, upload, or MongoDB insert fails
    """
    try:
        return _export_to_datalake(
            gdal=context.resources.gdal,
            postgis=context.resources.postgis,
            minio=context.resources.minio,
            mongodb=context.resources.mongodb,
            transform_result=transform_result,
            run_id=context.run_id,
            log=context.log,
        )
    finally:
        # Cleanup schema after export completes (success or failure)
        # This ensures PostGIS remains transient compute only
        try:
            run_id = context.run_id
            mapping = RunIdSchemaMapping.from_run_id(run_id)
            schema = mapping.schema_name
            
            postgis = context.resources.postgis
            context.log.info(f"Cleaning up ephemeral schema: {schema}")
            postgis._drop_schema(schema)
            context.log.info(f"Successfully dropped schema: {schema}")
        except Exception as e:
            # Log but don't raise - cleanup failures shouldn't break the pipeline
            context.log.warning(f"Failed to cleanup schema: {e}")
