# =============================================================================
# Join Ops - Spatial Join Operations
# =============================================================================
# Helper functions for joining tabular and spatial datasets.
# Used by joined_spatial_asset to combine tabular (primary) with spatial (secondary).
# =============================================================================

import tempfile
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import pyarrow.parquet as pq

from libs.models import Manifest, JoinConfig, AssetKind, Asset
from libs.s3_utils import s3_to_vsis3


def _resolve_join_assets(
    mongodb,
    manifest: Dict[str, Any],
    log,
) -> Dict[str, Any]:
    """
    Resolve primary (tabular) and secondary (spatial) assets for join.

    Args:
        mongodb: MongoDBResource instance
        manifest: Validated manifest dict
        log: Logger

    Returns:
        Dict with:
        - manifest: Original manifest
        - primary_type: "tabular"
        - secondary_asset: Asset model of spatial parent (as dict)
        - join_config: JoinConfig from manifest (as dict)

    Raises:
        ValueError: If target_asset_id is None or asset not found
    """
    validated = Manifest(**manifest)
    join_config = validated.metadata.join_config

    # Fail-fast if no join_config or target_asset_id
    if join_config is None:
        raise ValueError(
            f"Manifest {validated.batch_id} has intent 'join_datasets' "
            "but metadata.join_config is missing"
        )

    if join_config.target_asset_id is None:
        raise ValueError(
            f"Manifest {validated.batch_id} requires join_config.target_asset_id "
            "for explicit join"
        )

    # Look up secondary asset
    secondary_asset = mongodb.get_asset_by_id(join_config.target_asset_id)
    if secondary_asset is None:
        raise ValueError(
            f"Secondary asset not found: {join_config.target_asset_id}"
        )

    # Validate secondary is spatial
    if secondary_asset.kind != AssetKind.SPATIAL:
        raise ValueError(
            f"Secondary asset must be spatial, got {secondary_asset.kind.value}"
        )

    log.info(f"Resolved secondary asset: {secondary_asset.dataset_id} v{secondary_asset.version}")
    log.info(f"Join config: LEFT={join_config.left_key}, RIGHT={join_config.right_key}, HOW={join_config.how}")

    return {
        "manifest": manifest,
        "primary_type": "tabular",
        "secondary_asset": secondary_asset.model_dump(mode="json"),
        "join_config": join_config.model_dump(mode="json"),
    }


def _load_tabular_to_postgis(
    minio,
    postgis,
    s3_key: str,
    schema: str,
    table_name: str,
    log,
) -> Dict[str, Any]:
    """
    Load Parquet file from S3 into PostGIS as a regular table.

    Uses PyArrow to read Parquet and pandas to_sql for efficient bulk insert.

    Args:
        minio: MinIOResource
        postgis: PostGISResource
        s3_key: S3 key of the Parquet file
        schema: Target PostGIS schema
        table_name: Target table name
        log: Logger

    Returns:
        Dict with schema, table, columns, row_count
    """
    # Download Parquet to temp file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    temp_path = temp_file.name
    temp_file.close()

    try:
        minio.download_from_lake(s3_key, temp_path)
        log.info(f"Downloaded {s3_key} to {temp_path}")

        # Read with PyArrow
        table = pq.read_table(temp_path)
        columns = table.column_names
        log.info(f"Read Parquet: {len(table)} rows, {len(columns)} columns")

        # Convert to pandas DataFrame for to_sql
        df = table.to_pandas()

        # Create table in PostGIS using pandas to_sql
        engine = postgis.get_engine()
        df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=10000,
        )
        log.info(f"Inserted {len(df)} rows into {schema}.{table_name}")

        return {
            "schema": schema,
            "table": table_name,
            "columns": columns,
            "row_count": len(table),
        }
    finally:
        Path(temp_path).unlink(missing_ok=True)


def _load_tabular_to_postgis_from_file(
    postgis,
    parquet_path: str,
    schema: str,
    table_name: str,
    log,
) -> Dict[str, Any]:
    """
    Load Parquet file from local path into PostGIS as a regular table.

    Uses PyArrow to read Parquet and pandas to_sql for efficient bulk insert.

    Args:
        postgis: PostGISResource
        parquet_path: Local path to Parquet file
        schema: Target PostGIS schema
        table_name: Target table name
        log: Logger

    Returns:
        Dict with schema, table, columns, row_count
    """
    # Read with PyArrow
    table = pq.read_table(parquet_path)
    columns = table.column_names
    log.info(f"Read Parquet: {len(table)} rows, {len(columns)} columns")

    # Convert to pandas DataFrame for to_sql
    df = table.to_pandas()

    # Create table in PostGIS using pandas to_sql
    engine = postgis.get_engine()
    df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=10000,
    )
    log.info(f"Inserted {len(df)} rows into {schema}.{table_name}")

    return {
        "schema": schema,
        "table": table_name,
        "columns": columns,
        "row_count": len(table),
    }


def _load_geoparquet_to_postgis(
    gdal,
    postgis,
    minio,
    s3_key: str,
    schema: str,
    table_name: str,
    log,
) -> Dict[str, Any]:
    """
    Load GeoParquet from S3 into PostGIS using ogr2ogr.

    Args:
        gdal: GDALResource
        postgis: PostGISResource
        minio: MinIOResource
        s3_key: S3 key of the GeoParquet file
        schema: Target PostGIS schema
        table_name: Target table name
        log: Logger

    Returns:
        Dict with schema, table, geom_column
    """
    # Build vsis3 path for GDAL
    full_s3_path = f"s3://{minio.lake_bucket}/{s3_key}"
    vsis3_path = s3_to_vsis3(full_s3_path)

    # Build PostgreSQL connection string
    pg_conn_str = (
        f"PG:host={postgis.host} "
        f"dbname={postgis.database} "
        f"user={postgis.user} "
        f"password={postgis.password}"
    )

    layer_name = f"{schema}.{table_name}"

    result = gdal.ogr2ogr(
        input_path=vsis3_path,
        output_path=pg_conn_str,
        output_format="PostgreSQL",
        layer_name=layer_name,
        options={"-overwrite": "", "-lco": "GEOMETRY_NAME=geom"},
    )

    if not result.success:
        raise RuntimeError(f"ogr2ogr failed: {result.stderr}")

    log.info(f"Loaded GeoParquet to {schema}.{table_name}")

    return {
        "schema": schema,
        "table": table_name,
        "geom_column": "geom",
    }


def _execute_spatial_join(
    postgis,
    tabular_info: Dict[str, Any],
    spatial_info: Dict[str, Any],
    join_config: Dict[str, Any],
    output_table: str,
    log,
) -> Dict[str, Any]:
    """
    Execute SQL JOIN between tabular and spatial tables.

    Tabular is LEFT, Spatial is RIGHT. Geometry comes from spatial.

    Args:
        postgis: PostGISResource
        tabular_info: Dict with schema, table, columns
        spatial_info: Dict with schema, table, geom_column
        join_config: JoinConfig as dict
        output_table: Name of joined output table
        log: Logger

    Returns:
        Dict compatible with export_to_datalake (schema, table, bounds, geom_column)
    """
    schema = tabular_info["schema"]
    t_table = tabular_info["table"]
    s_table = spatial_info["table"]
    geom_col = spatial_info.get("geom_column", "geom")

    left_key = join_config["left_key"]
    right_key = join_config.get("right_key") or left_key
    how = join_config.get("how", "left").upper()

    # Map join types
    join_type_map = {
        "LEFT": "LEFT JOIN",
        "INNER": "INNER JOIN",
        "RIGHT": "RIGHT JOIN",
        "OUTER": "FULL OUTER JOIN",
    }
    join_clause = join_type_map.get(how, "LEFT JOIN")

    # Build SQL with explicit column selection
    # Avoid duplicate columns by aliasing
    sql = f'''
    CREATE TABLE "{schema}"."{output_table}" AS
    SELECT
        t.*,
        s."{geom_col}" AS geom
    FROM "{schema}"."{t_table}" t
    {join_clause} "{schema}"."{s_table}" s
    ON t."{left_key}" = s."{right_key}";
    '''

    postgis.execute_sql(sql, schema)
    log.info(f"Created joined table {schema}.{output_table}")

    # Get bounds from joined table
    bounds = postgis.get_table_bounds(schema, output_table, geom_column="geom")
    bounds_dict = None
    if bounds:
        bounds_dict = {
            "minx": bounds.minx,
            "miny": bounds.miny,
            "maxx": bounds.maxx,
            "maxy": bounds.maxy,
        }

    return {
        "schema": schema,
        "table": output_table,
        "bounds": bounds_dict,
        "geom_column": "geom",
    }


def _export_joined_to_datalake(
    gdal,
    postgis,
    minio,
    mongodb,
    transform_result: Dict[str, Any],
    dataset_id: str,
    run_id: str,
    log,
) -> Dict[str, Any]:
    """
    Export joined spatial data to data lake and register in MongoDB.

    Similar to _export_to_datalake but creates Asset with kind=JOINED.

    Args:
        gdal: GDALResource instance
        postgis: PostGISResource instance
        minio: MinIOResource instance
        mongodb: MongoDBResource instance
        transform_result: Transform result dict with schema, table, bounds, manifest, crs
        dataset_id: Dataset identifier (from tags or generated)
        run_id: Dagster run ID
        log: Logger instance

    Returns:
        Asset info dict with asset_id, s3_key, dataset_id, version, content_hash, run_id
    """
    import hashlib
    import tempfile
    import uuid
    from datetime import datetime, timezone
    from pathlib import Path

    from libs.models import Asset, AssetKind, AssetMetadata, Bounds, OutputFormat, CRS

    schema = transform_result["schema"]
    table = transform_result["table"]
    manifest = transform_result["manifest"]
    bounds_dict = transform_result.get("bounds")
    crs = transform_result.get("crs")

    if not crs:
        raise ValueError("CRS is required for joined assets")

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
        result = gdal.ogr2ogr(
            input_path=pg_conn_str,
            output_path=temp_file_path,
            output_format="Parquet",
            target_crs=crs,
        )

        # Check for failure
        if not result.success:
            raise RuntimeError(f"ogr2ogr export failed: {result.stderr}")

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
        validated_manifest = Manifest(**manifest)
        manifest_tags = validated_manifest.metadata.tags.copy()
        asset_metadata = AssetMetadata(
            title=validated_manifest.metadata.project or dataset_id,
            description=validated_manifest.metadata.description,
            source=None,
            license=None,
            tags=manifest_tags,
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
            kind=AssetKind.JOINED,
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

