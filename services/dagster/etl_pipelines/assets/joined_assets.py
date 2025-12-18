"""
Derived assets - joined spatial asset (Phase 4).

Produces a spatialized tabular dataset by joining incoming tabular data (left)
with an existing spatial asset (right) and exporting GeoParquet to the data lake.
"""

from typing import Any, Dict

from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset

from libs.models import Manifest
from ..partitions import dataset_partitions

from ..ops.join_ops import (
    _choose_dataset_id,
    _execute_spatial_join,
    _export_joined_to_datalake,
    _load_geoparquet_to_postgis,
    _load_tabular_arrow_to_postgis,
    _load_tabular_parquet_to_postgis,
    _resolve_join_assets,
)


@asset(
    group_name="derived",
    compute_kind="join",
    required_resource_keys={"gdal", "postgis", "minio", "mongodb"},
    description="Derived asset: joins tabular (primary) with spatial (secondary) datasets.",
    partitions_def=dataset_partitions,
    deps=[AssetKey("raw_spatial_asset"), AssetKey("raw_tabular_asset")],
)
def joined_spatial_asset(
    context: AssetExecutionContext,
    raw_manifest_json: Dict[str, Any],
) -> Dict[str, Any]:
    """Derived asset for spatial joins.

    Joins a tabular asset (left) with a spatial asset (right) to produce
    a spatialized output GeoParquet.

    Workflow:
    1. Resolve both spatial and tabular assets from MongoDB
    2. Load tabular asset (data-lake parquet) to PostGIS
    3. Load spatial asset (data-lake geoparquet) to PostGIS
    4. Execute SQL JOIN (tabular LEFT JOIN spatial by default)
    5. Export joined GeoParquet to data lake, register Asset in MongoDB
    6. Record lineage edges (spatial→joined, tabular→joined)
    """
    # Register dynamic partition key (idempotent)
    partition_key = context.partition_key
    if partition_key:
        context.instance.add_dynamic_partitions(
            partitions_def_name=dataset_partitions.name,
            partition_keys=[partition_key],
        )

    validated_manifest = Manifest(**raw_manifest_json)
    join_resolution = _resolve_join_assets(
        mongodb=context.resources.mongodb,
        manifest=validated_manifest.model_dump(mode="json"),
        log=context.log,
    )

    join_config = join_resolution["join_config"]
    spatial_asset = join_resolution["spatial_asset"]
    spatial_asset_id = join_resolution["spatial_asset_id"]
    tabular_asset = join_resolution["tabular_asset"]
    tabular_asset_id = join_resolution["tabular_asset_id"]

    left_key = join_config.left_key
    right_key = join_config.right_key or join_config.left_key

    with context.resources.postgis.ephemeral_schema(context.run_id) as schema:
        tabular_postgis = _load_tabular_parquet_to_postgis(
            minio=context.resources.minio,
            postgis=context.resources.postgis,
            s3_key=tabular_asset.s3_key,
            schema=schema,
            table_name="tabular_parent",
            log=context.log,
        )

        if left_key not in tabular_postgis["columns"]:
            raise ValueError(
                f"Join left_key '{left_key}' not found in tabular asset columns: {tabular_postgis['columns']}"
            )

        spatial_postgis = _load_geoparquet_to_postgis(
            gdal=context.resources.gdal,
            postgis=context.resources.postgis,
            minio=context.resources.minio,
            s3_key=spatial_asset.s3_key,
            schema=schema,
            table_name="spatial_parent",
            log=context.log,
        )

        join_result = _execute_spatial_join(
            postgis=context.resources.postgis,
            schema=schema,
            tabular_table=tabular_postgis["table"],
            spatial_table=spatial_postgis["table"],
            left_key=left_key,
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
            crs=str(spatial_asset.crs),
            bounds_dict=join_result["bounds"],
            dataset_id=dataset_id,
            run_id=context.run_id,
            log=context.log,
        )

    joined_asset_id = asset_info["asset_id"]

    # spatial_asset_id and tabular_asset_id from join_config are MongoDB ObjectId strings
    # Use them directly for lineage recording (no need to look up by dataset_id)
    spatial_object_id = spatial_asset_id
    tabular_object_id = tabular_asset_id

    spatial_lineage_id = context.resources.mongodb.insert_lineage(
        source_asset_id=spatial_object_id,
        target_asset_id=joined_asset_id,
        dagster_run_id=context.run_id,
        transformation="spatial_join",
        parameters={
            "role": "spatial_parent",
            "right_key": right_key,
            "how": join_config.how,
        },
    )
    context.log.info(f"Recorded spatial lineage edge: {spatial_lineage_id}")

    tabular_lineage_id = context.resources.mongodb.insert_lineage(
        source_asset_id=tabular_object_id,
        target_asset_id=joined_asset_id,
        dagster_run_id=context.run_id,
        transformation="spatial_join",
        parameters={
            "role": "tabular_parent",
            "left_key": left_key,
            "how": join_config.how,
        },
    )
    context.log.info(f"Recorded tabular lineage edge: {tabular_lineage_id}")

    context.add_output_metadata(
        {
            "dataset_id": MetadataValue.text(asset_info["dataset_id"]),
            "version": MetadataValue.int(asset_info["version"]),
            "s3_key": MetadataValue.text(asset_info["s3_key"]),
            "asset_id": MetadataValue.text(str(asset_info["asset_id"])),
            "spatial_parent_asset_id": MetadataValue.text(spatial_asset_id),
            "spatial_parent_dataset_id": MetadataValue.text(spatial_asset.dataset_id),
            "tabular_parent_asset_id": MetadataValue.text(tabular_asset_id),
            "tabular_parent_dataset_id": MetadataValue.text(tabular_asset.dataset_id),
            "join_type": MetadataValue.text(join_config.how),
        }
    )

    return asset_info
