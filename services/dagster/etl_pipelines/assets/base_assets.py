# =============================================================================
# Base Assets - Graph-backed ingestion assets
# =============================================================================
# Graph-backed assets for spatial and tabular ingestion pipelines.
# These assets wrap the existing ops and provide asset-level metadata.
# =============================================================================

from typing import Dict, Any

from dagster import asset, AssetExecutionContext, Config, MetadataValue

from libs.models import Manifest, ManifestRecord, ManifestStatus
from ..partitions import dataset_partitions
from ..ops.load_op import _load_files_to_postgis
from ..ops.transform_op import _spatial_transform
from ..ops.export_op import _export_to_datalake
from ..ops.tabular_ops import (
    _download_tabular_from_landing,
    _load_and_clean_tabular,
    _export_tabular_parquet_to_datalake,
)


class ManifestConfig(Config):
    """Config schema for manifest input to assets."""

    manifest: Dict[str, Any]


@asset(
    group_name="ingestion",
    compute_kind="validation",
    required_resource_keys={"mongodb"},
    description="Validates manifest JSON and initializes run in MongoDB.",
)
def raw_manifest_json(
    context: AssetExecutionContext, config: ManifestConfig
) -> Dict[str, Any]:
    """
    Root asset that validates manifest JSON and initializes the run in MongoDB.

    Receives manifest dict from run config (set by sensor) and validates it
    against the Manifest model. Also creates the run document in MongoDB to
    ensure it exists before downstream assets attempt to link to it.

    Args:
        context: Dagster asset execution context
        config: Config containing manifest dict

    Returns:
        Dict containing:
        - manifest: Normalized manifest dict (via model_dump(mode="json"))
        - run_id: MongoDB run document ObjectId

    Raises:
        ValueError: If manifest validation fails
    """
    mongodb = context.resources.mongodb
    dagster_run_id = context.run_id
    tags = context.run.tags

    # Validate manifest
    validated_manifest = Manifest(**config.manifest)

    # Log key fields
    context.log.info(f"Validated manifest: {validated_manifest.batch_id}")
    context.log.info(f"Intent: {validated_manifest.intent}")
    context.log.info(f"Files: {len(validated_manifest.files)} file(s)")

    batch_id = validated_manifest.batch_id
    job_name = context.job_name
    partition_key = tags.get("partition_key")

    # Ensure manifest exists in MongoDB with status=RUNNING
    existing_manifest = mongodb.get_manifest(batch_id)
    if existing_manifest is None:
        record = ManifestRecord.from_manifest(
            validated_manifest, status=ManifestStatus.RUNNING
        )
        mongodb.insert_manifest(record)
        context.log.info(f"Created manifest document for batch_id={batch_id}")
    else:
        mongodb.update_manifest_status(batch_id=batch_id, status=ManifestStatus.RUNNING)
        context.log.info(f"Updated manifest {batch_id} to RUNNING")

    # Create run document
    run_id = mongodb.insert_run(
        dagster_run_id=dagster_run_id,
        batch_id=batch_id,
        job_name=job_name,
        partition_key=partition_key,
    )
    context.log.info(f"Created run document: {run_id}")

    # Return normalized dict with run_id
    return {
        "manifest": validated_manifest.model_dump(mode="json"),
        "run_id": run_id,
        "dagster_run_id": dagster_run_id,
    }


@asset(
    group_name="ingestion",
    compute_kind="spatial",
    required_resource_keys={"gdal", "postgis", "minio", "mongodb"},
    description="Spatial ingestion asset: loads, transforms, and exports spatial data.",
    partitions_def=dataset_partitions,
    deps=[raw_manifest_json],
)
def raw_spatial_asset(
    context: AssetExecutionContext, raw_manifest_json: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Graph-backed asset for spatial data ingestion.

    Wraps the spatial ingestion pipeline:
    1. load_to_postgis: Loads spatial data from landing zone to PostGIS
    2. spatial_transform: Executes spatial transformations
    3. export_to_datalake: Exports to data lake and registers in MongoDB

    Important: PostGIS is transient compute only, so we must always drop the
    per-run ephemeral schema (proc_<run_id>) after processing.

    Args:
        context: Dagster asset execution context
        raw_manifest_json: Dict from raw_manifest_json containing manifest, run_id, dagster_run_id

    Returns:
        Asset info dict with dataset_id, version, s3_key, asset_id, content_hash

    Raises:
        RuntimeError: If any step in the pipeline fails
    """
    # Extract manifest and run info from upstream asset
    manifest = raw_manifest_json["manifest"]
    run_id = raw_manifest_json["run_id"]
    dagster_run_id = raw_manifest_json["dagster_run_id"]

    # Register dynamic partition key (idempotent)
    partition_key = context.partition_key
    if partition_key:
        context.instance.add_dynamic_partitions(
            partitions_def_name=dataset_partitions.name,
            partition_keys=[partition_key],
        )

    schema_info: Dict[str, Any] | None = None

    try:
        # Step 1: Load to PostGIS
        context.log.info("Loading spatial data to PostGIS")
        schema_info = _load_files_to_postgis(
            gdal=context.resources.gdal,
            postgis=context.resources.postgis,
            manifest=manifest,
            run_id=dagster_run_id,  # Uses dagster_run_id for schema naming
            log=context.log,
            geom_column_name="geom",
        )

        # Step 2: Transform
        context.log.info("Transforming spatial data")
        transform_result = _spatial_transform(
            postgis=context.resources.postgis,
            schema_info=schema_info,
            log=context.log,
        )

        # Step 3: Export to data lake
        context.log.info("Exporting to data lake")
        asset_info = _export_to_datalake(
            gdal=context.resources.gdal,
            postgis=context.resources.postgis,
            minio=context.resources.minio,
            mongodb=context.resources.mongodb,
            transform_result=transform_result,
            dagster_run_id=dagster_run_id,
            run_id=run_id,
            log=context.log,
        )

        # Attach metadata to asset
        context.add_output_metadata(
            {
                "dataset_id": MetadataValue.text(asset_info["dataset_id"]),
                "version": MetadataValue.int(asset_info["version"]),
                "s3_key": MetadataValue.text(asset_info["s3_key"]),
                "asset_id": MetadataValue.text(str(asset_info["asset_id"])),
                "content_hash": MetadataValue.text(
                    asset_info["content_hash"][:20] + "..."
                ),
                "run_id": MetadataValue.text(asset_info["run_id"]),
            }
        )

        return asset_info

    finally:
        # Always cleanup ephemeral PostGIS schema (architectural law)
        if schema_info and schema_info.get("schema"):
            schema = schema_info["schema"]
            context.log.info(f"Cleaning up ephemeral schema: {schema}")
            context.resources.postgis._drop_schema(schema)


@asset(
    group_name="ingestion",
    compute_kind="tabular",
    required_resource_keys={"minio", "mongodb"},
    description="Tabular ingestion asset: downloads, cleans, and exports tabular data.",
    partitions_def=dataset_partitions,
    deps=[raw_manifest_json],
)
def raw_tabular_asset(
    context: AssetExecutionContext, raw_manifest_json: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Graph-backed asset for tabular data ingestion.

    Wraps the tabular ingestion pipeline:
    1. download_tabular_from_landing: Downloads CSV from landing zone
    2. load_and_clean_tabular: Loads CSV and cleans headers
    3. export_tabular_parquet_to_datalake: Exports to Parquet and registers in MongoDB

    Args:
        context: Dagster asset execution context
        raw_manifest_json: Dict from raw_manifest_json containing manifest, run_id, dagster_run_id

    Returns:
        Asset info dict with dataset_id, version, s3_key, asset_id, content_hash

    Raises:
        RuntimeError: If any step in the pipeline fails
        ValueError: If manifest has multiple files (tabular is single-file only)
    """
    # Extract manifest and run info from upstream asset
    manifest = raw_manifest_json["manifest"]
    run_id = raw_manifest_json["run_id"]
    dagster_run_id = raw_manifest_json["dagster_run_id"]

    # Register dynamic partition key (idempotent)
    partition_key = context.partition_key
    if partition_key:
        context.instance.add_dynamic_partitions(
            partitions_def_name=dataset_partitions.name,
            partition_keys=[partition_key],
        )

    # Step 1: Download from landing zone
    context.log.info("Downloading tabular data from landing zone")
    download_result = _download_tabular_from_landing(
        minio=context.resources.minio,
        manifest=manifest,
        log=context.log,
    )

    # Step 2: Load and clean
    context.log.info("Loading and cleaning tabular data")
    table_info = _load_and_clean_tabular(
        download_result=download_result,
        log=context.log,
    )

    # Step 3: Export to data lake
    context.log.info("Exporting to data lake")
    asset_info = _export_tabular_parquet_to_datalake(
        minio=context.resources.minio,
        mongodb=context.resources.mongodb,
        table_info=table_info,
        dagster_run_id=dagster_run_id,
        run_id=run_id,
        log=context.log,
    )

    # Attach metadata to asset
    context.add_output_metadata(
        {
            "dataset_id": MetadataValue.text(asset_info["dataset_id"]),
            "version": MetadataValue.int(asset_info["version"]),
            "s3_key": MetadataValue.text(asset_info["s3_key"]),
            "asset_id": MetadataValue.text(str(asset_info["asset_id"])),
            "content_hash": MetadataValue.text(asset_info["content_hash"][:20] + "..."),
            "run_id": MetadataValue.text(asset_info["run_id"]),
        }
    )

    return asset_info
