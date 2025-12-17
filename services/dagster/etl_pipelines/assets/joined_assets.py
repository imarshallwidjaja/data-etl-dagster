# =============================================================================
# Joined Assets - Spatial Join Asset
# =============================================================================
# Derived asset that joins tabular (primary) with spatial (secondary) datasets.
# =============================================================================

import tempfile
import uuid
from pathlib import Path
from typing import Dict, Any

import pyarrow.parquet as pq
from dagster import asset, AssetExecutionContext, Config, MetadataValue

from libs.models import Manifest, Asset, AssetKind
from libs.spatial_utils import RunIdSchemaMapping
from ..ops.join_ops import (
    _resolve_join_assets,
    _load_tabular_to_postgis_from_file,
    _load_geoparquet_to_postgis,
    _execute_spatial_join,
    _export_joined_to_datalake,
)
from ..ops.tabular_ops import (
    _download_tabular_from_landing,
    _load_and_clean_tabular,
)


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
        # Write Arrow table to temp parquet first
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
            Path(temp_parquet.name).unlink(missing_ok=True)

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

        # Build transform_result compatible with _export_joined_to_datalake
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

        # Lineage: Spatial parent -> Joined
        # The target_asset_id in join_config is already the MongoDB ObjectId string
        source_asset_id = join_config["target_asset_id"]

        context.resources.mongodb.insert_lineage(
            source_asset_id=source_asset_id,
            target_asset_id=asset_info["asset_id"],
            dagster_run_id=run_id,
            transformation="spatial_join",
            parameters={
                "left_key": join_config["left_key"],
                "right_key": join_config.get("right_key") or join_config["left_key"],
                "how": join_config.get("how", "left"),
            },
        )
        context.log.info(f"Recorded lineage: {source_asset_id} -> {asset_info['asset_id']}")

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

