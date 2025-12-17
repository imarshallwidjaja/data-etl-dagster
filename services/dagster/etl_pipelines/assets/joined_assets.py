"""
Derived assets - joined spatial asset (Phase 4).

Produces a spatialized tabular dataset by joining incoming tabular data (left)
with an existing spatial asset (right) and exporting GeoParquet to the data lake.
"""

from typing import Any, Dict

from dagster import AssetExecutionContext, MetadataValue, asset

from libs.models import Manifest
from ..partitions import dataset_partitions

from ..ops.join_ops import (
    _choose_dataset_id,
    _execute_spatial_join,
    _export_joined_to_datalake,
    _load_geoparquet_to_postgis,
    _load_tabular_arrow_to_postgis,
    _resolve_join_assets,
)
from ..ops.tabular_ops import _download_tabular_from_landing, _load_and_clean_tabular


@asset(
    group_name="derived",
    compute_kind="join",
    required_resource_keys={"gdal", "postgis", "minio", "mongodb"},
    description="Derived asset: joins tabular (primary) with spatial (secondary) datasets.",
    partitions_def=dataset_partitions,
)
def joined_spatial_asset(
    context: AssetExecutionContext,
    raw_manifest_json: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Graph-backed asset for spatial joins.

    Workflow:
    1. Resolve secondary (spatial) asset from MongoDB
    2. Download and ingest incoming tabular data (landing-zone) to PostGIS
    3. Load spatial parent (data-lake) to PostGIS
    4. Execute SQL JOIN (tabular LEFT JOIN spatial by default)
    5. Export joined GeoParquet to data lake, register Asset in MongoDB
    6. Record lineage (spatial parent -> joined)
    """
    # Register dynamic partition key (idempotent)
    partition_key = context.partition_key
    if partition_key:
        context.instance.add_dynamic_partitions(
            partitions_def_name=dataset_partitions.name,
            partition_keys=[partition_key],
        )

    # Validate manifest structure (raw_manifest_json is already normalized upstream)
    validated_manifest = Manifest(**raw_manifest_json)
    join_resolution = _resolve_join_assets(
        mongodb=context.resources.mongodb,
        manifest=validated_manifest.model_dump(mode="json"),
        log=context.log,
    )

    join_config = join_resolution["join_config"]
    secondary_asset = join_resolution["secondary_asset"]
    secondary_asset_id = join_resolution["secondary_asset_id"]

    # Prepare tabular input
    download_result = _download_tabular_from_landing(
        minio=context.resources.minio,
        manifest=validated_manifest.model_dump(mode="json"),
        log=context.log,
    )
    table_info = _load_and_clean_tabular(download_result=download_result, log=context.log)

    left_key_clean = table_info.get("join_key_clean")
    if not left_key_clean:
        raise ValueError(
            "join_datasets requires metadata.join_config.left_key and it must exist in the input CSV headers"
        )

    right_key = join_config.right_key or join_config.left_key

    with context.resources.postgis.ephemeral_schema(context.run_id) as schema:
        # Load tables
        tabular_postgis = _load_tabular_arrow_to_postgis(
            postgis=context.resources.postgis,
            table=table_info["table"],
            schema=schema,
            table_name="tabular_parent",
            log=context.log,
        )

        spatial_postgis = _load_geoparquet_to_postgis(
            gdal=context.resources.gdal,
            postgis=context.resources.postgis,
            minio=context.resources.minio,
            s3_key=secondary_asset.s3_key,
            schema=schema,
            table_name="spatial_parent",
            log=context.log,
        )

        join_result = _execute_spatial_join(
            postgis=context.resources.postgis,
            schema=schema,
            tabular_table=tabular_postgis["table"],
            spatial_table=spatial_postgis["table"],
            left_key=left_key_clean,
            right_key=right_key,
            how=join_config.how,
            output_table="joined",
            log=context.log,
        )

        dataset_id = _choose_dataset_id(validated_manifest)
        asset_info = _export_joined_to_datalake(
            gdal=context.resources.gdal,
            postgis=context.resources.postgis,
            minio=context.resources.minio,
            mongodb=context.resources.mongodb,
            schema=schema,
            table=join_result["table"],
            manifest=validated_manifest.model_dump(mode="json"),
            crs=str(secondary_asset.crs),
            bounds_dict=join_result["bounds"],
            dataset_id=dataset_id,
            run_id=context.run_id,
            log=context.log,
        )

    # Record lineage (outside PostGIS schema context)
    lineage_id = context.resources.mongodb.insert_lineage(
        source_asset_id=secondary_asset_id,
        target_asset_id=asset_info["asset_id"],
        dagster_run_id=context.run_id,
        transformation="spatial_join",
        parameters={
            "left_key": join_config.left_key,
            "left_key_clean": left_key_clean,
            "right_key": right_key,
            "how": join_config.how,
        },
    )
    context.log.info(f"Recorded lineage edge: {lineage_id}")

    context.add_output_metadata(
        {
            "dataset_id": MetadataValue.text(asset_info["dataset_id"]),
            "version": MetadataValue.int(asset_info["version"]),
            "s3_key": MetadataValue.text(asset_info["s3_key"]),
            "asset_id": MetadataValue.text(str(asset_info["asset_id"])),
            "spatial_parent_asset_id": MetadataValue.text(secondary_asset_id),
            "spatial_parent_dataset_id": MetadataValue.text(secondary_asset.dataset_id),
            "join_type": MetadataValue.text(join_config.how),
        }
    )

    return asset_info


