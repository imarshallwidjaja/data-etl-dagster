from typing import Dict, Any
from dagster import asset, AssetExecutionContext, Config, MetadataValue

from libs.models import Manifest, Asset, AssetKind, AssetMetadata, OutputFormat, CRS, Bounds
from libs.spatial_utils import RunIdSchemaMapping
from ..ops.join_ops import (
    _resolve_join_assets,
    _load_tabular_to_postgis_from_file,
    _load_geoparquet_to_postgis,
    _execute_spatial_join,
)
from ..ops.export_op import _export_to_datalake


class JoinManifestConfig(Config):
    """Config schema for join manifest input."""
    manifest: Dict[str, Any]


@asset(
    group_name="derived",
    compute_kind="join",
    required_resource_keys={"gdal", "postgis", "minio", "mongodb"},
    description="Derived asset: joins tabular (primary) with spatial (secondary) datasets.",
)
def joined_spatial_asset(
    context: AssetExecutionContext,
    config: JoinManifestConfig,
) -> Dict[str, Any]:
    """
    Graph-backed asset for spatial joins.

    Workflow:
    1. Resolve secondary (spatial) asset from MongoDB
    2. Download and ingest incoming tabular data
    3. Load spatial parent from data lake to PostGIS
    4. Execute SQL JOIN (tabular LEFT JOIN spatial)
    5. Export joined GeoParquet to data lake
    6. Record lineage in MongoDB

    Args:
        context: Dagster asset execution context
        config: Config containing manifest dict

    Returns:
        Asset info dict with dataset_id, version, s3_key, asset_id
    """
    manifest = config.manifest
    run_id = context.run_id

    # Step 1: Resolve assets
    context.log.info("Resolving join assets...")
    resolution = _resolve_join_assets(
        mongodb=context.resources.mongodb,
        manifest=manifest,
        log=context.log,
    )

    secondary_asset = Asset(**resolution["secondary_asset"])
    join_config = resolution["join_config"]

    # Create ephemeral schema
    mapping = RunIdSchemaMapping.from_run_id(run_id)
    schema = mapping.schema_name
    context.resources.postgis._create_schema(schema)
    context.log.info(f"Created ephemeral schema: {schema}")

    try:
        # Step 2: Load tabular (primary) - process from manifest
        context.log.info("Processing tabular data from manifest...")
        from ..ops.tabular_ops import _download_tabular_from_landing, _load_and_clean_tabular

        download_result = _download_tabular_from_landing(
            minio=context.resources.minio,
            manifest=manifest,
            log=context.log,
        )

        table_info = _load_and_clean_tabular(
            download_result=download_result,
            log=context.log,
        )

        # Load cleaned tabular to PostGIS
        # (We need a temp parquet for this - or load Arrow directly)
        # Simplified: use pandas to_sql from Arrow table
        import tempfile
        import pyarrow.parquet as pq

        temp_parquet = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        pq.write_table(table_info["table"], temp_parquet.name)
        temp_parquet.close()

        try:
            tabular_postgis = _load_tabular_to_postgis_from_file(
                postgis=context.resources.postgis,
                parquet_path=temp_parquet.name,
                schema=schema,
                table_name="tabular_parent",
                log=context.log,
            )
        finally:
            import os
            os.unlink(temp_parquet.name)

        # Step 3: Load spatial (secondary) from data lake
        context.log.info(f"Loading spatial parent: {secondary_asset.s3_key}")
        spatial_postgis = _load_geoparquet_to_postgis(
            gdal=context.resources.gdal,
            postgis=context.resources.postgis,
            minio=context.resources.minio,
            s3_key=secondary_asset.s3_key,
            schema=schema,
            table_name="spatial_parent",
            log=context.log,
        )

        # Step 4: Execute JOIN
        context.log.info("Executing spatial join...")
        join_result = _execute_spatial_join(
            postgis=context.resources.postgis,
            tabular_info=tabular_postgis,
            spatial_info=spatial_postgis,
            join_config=join_config,
            output_table="joined",
            log=context.log,
        )

        # Step 5: Export to data lake
        context.log.info("Exporting joined data to data lake...")

        # Build transform_result compatible with _export_to_datalake
        validated_manifest = Manifest(**manifest)
        transform_result = {
            "schema": schema,
            "table": "joined",
            "manifest": manifest,
            "bounds": join_result["bounds"],
            "crs": secondary_asset.crs,  # Inherit CRS from spatial parent
        }

        # Generate dataset_id (user tag or UUID)
        dataset_id = validated_manifest.metadata.tags.get("dataset_id")
        if not dataset_id or not isinstance(dataset_id, str):
            import uuid
            dataset_id = f"dataset_{uuid.uuid4().hex[:12]}"

        # Custom export for joined asset (uses AssetKind.JOINED)
        asset_info = _export_joined_to_datalake(
            gdal=context.resources.gdal,
            postgis=context.resources.postgis,
            minio=context.resources.minio,
            mongodb=context.resources.mongodb,
            transform_result=transform_result,
            dataset_id=dataset_id,
            run_id=run_id,
            log=context.log,
        )

        # Step 6: Record lineage
        context.log.info("Recording lineage...")

        # Lineage: Tabular parent -> Joined
        # Note: Tabular parent was just created, we need its asset_id
        # For now, we record spatial parent lineage
        context.resources.mongodb.insert_lineage(
            source_asset_id=str(secondary_asset.id) if hasattr(secondary_asset, 'id') else join_config["target_asset_id"],
            target_asset_id=asset_info["asset_id"],
            dagster_run_id=run_id,
            transformation="spatial_join",
            parameters={
                "left_key": join_config["left_key"],
                "right_key": join_config.get("right_key"),
                "how": join_config.get("how", "left"),
            },
        )
        context.log.info(f"Recorded lineage: {join_config['target_asset_id']} -> {asset_info['asset_id']}")

        # Attach metadata
        context.add_output_metadata({
            "dataset_id": MetadataValue.text(asset_info["dataset_id"]),
            "version": MetadataValue.int(asset_info["version"]),
            "s3_key": MetadataValue.text(asset_info["s3_key"]),
            "asset_id": MetadataValue.text(str(asset_info["asset_id"])),
            "spatial_parent": MetadataValue.text(secondary_asset.dataset_id),
            "join_type": MetadataValue.text(join_config.get("how", "left")),
        })

        return asset_info

    finally:
        # Cleanup ephemeral schema
        context.log.info(f"Cleaning up schema: {schema}")
        context.resources.postgis._drop_schema(schema)


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
    Export joined data to data lake with AssetKind.JOINED.

    Similar to _export_to_datalake but creates JOINED assets.

    Args:
        gdal: GDALResource instance
        postgis: PostGISResource instance
        minio: MinIOResource instance
        mongodb: MongoDBResource instance
        transform_result: Transform result dict
        dataset_id: Dataset identifier
        run_id: Dagster run ID
        log: Logger instance

    Returns:
        Asset info dict with asset_id, s3_key, dataset_id, version, content_hash, run_id
    """
    import hashlib
    import tempfile
    from datetime import datetime, timezone
    from pathlib import Path

    schema = transform_result["schema"]
    table = transform_result["table"]
    manifest = transform_result["manifest"]
    bounds_dict = transform_result["bounds"]
    crs = transform_result["crs"]

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
        from ..resources.gdal_resource import GDALResult
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
        # Populate tags from manifest metadata.tags
        manifest_tags = manifest["metadata"].get("tags", {})
        asset_metadata = AssetMetadata(
            title=manifest["metadata"].get("project", dataset_id),
            description=manifest["metadata"].get("description"),
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
            kind=AssetKind.JOINED,  # Key difference: JOINED instead of SPATIAL
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
