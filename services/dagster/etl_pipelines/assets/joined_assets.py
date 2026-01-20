"""
Derived assets - joined spatial asset (Phase 4).

Produces a spatialized tabular dataset by joining incoming tabular data (left)
with an existing spatial asset (right) and exporting GeoParquet to the data lake.
"""

from pathlib import Path
import tempfile
from typing import Any, Dict

from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset
import pyarrow.parquet as pq

from libs.models import Manifest
from ..partitions import dataset_partitions

from ..ops.duckdb_settings import build_duckdb_join_settings
from ..ops.join_ops import (
    _choose_dataset_id,
    _execute_duckdb_join,
    _export_duckdb_join_to_datalake,
    _resolve_join_assets,
)


@asset(
    group_name="derived",
    compute_kind="join",
    required_resource_keys={"minio", "mongodb"},
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
    2. Validate join keys against Parquet schemas
    3. Execute DuckDB join over Parquet/GeoParquet inputs
    4. Export joined GeoParquet to data lake, register Asset in MongoDB
    5. Record lineage edges (spatial→joined, tabular→joined)
    """
    # Register dynamic partition key (idempotent)
    partition_key = context.partition_key
    if partition_key:
        context.instance.add_dynamic_partitions(
            partitions_def_name=dataset_partitions.name,
            partition_keys=[partition_key],
        )

    # Extract manifest and run info from upstream asset
    manifest_dict = raw_manifest_json["manifest"]
    run_id = raw_manifest_json["run_id"]
    dagster_run_id = raw_manifest_json["dagster_run_id"]

    validated_manifest = Manifest(**manifest_dict)
    join_resolution = _resolve_join_assets(
        mongodb=context.resources.mongodb,
        manifest=validated_manifest.model_dump(mode="json"),
        log=context.log,
    )

    join_config = join_resolution["join_config"]
    spatial_asset = join_resolution["spatial_asset"]
    spatial_object_id = join_resolution["spatial_object_id"]
    tabular_asset = join_resolution["tabular_asset"]
    tabular_object_id = join_resolution["tabular_object_id"]

    left_key = join_config.left_key
    right_key = join_config.right_key or join_config.left_key

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        minio = context.resources.minio

        tabular_schema_path = temp_path / "tabular.parquet"
        spatial_schema_path = temp_path / "spatial.parquet"
        minio.download_from_lake(tabular_asset.s3_key, tabular_schema_path)
        minio.download_from_lake(spatial_asset.s3_key, spatial_schema_path)

        tabular_schema = pq.read_schema(tabular_schema_path)
        if left_key not in tabular_schema.names:
            raise ValueError(
                "Join left_key "
                f"'{left_key}' not found in tabular asset columns: {tabular_schema.names}"
            )

        spatial_schema = pq.read_schema(spatial_schema_path)
        if right_key not in spatial_schema.names:
            raise ValueError(
                "Join right_key "
                f"'{right_key}' not found in spatial asset columns: {spatial_schema.names}"
            )

        join_settings = build_duckdb_join_settings(
            minio=minio,
            temp_dir=temp_path,
        )
        output_path = temp_path / "joined.parquet"

        join_result = _execute_duckdb_join(
            tabular_path=f"s3://{minio.lake_bucket}/{tabular_asset.s3_key}",
            spatial_path=f"s3://{minio.lake_bucket}/{spatial_asset.s3_key}",
            left_key=left_key,
            right_key=right_key,
            how=join_config.how,
            output_path=str(output_path),
            temp_dir=temp_dir,
            s3_settings=join_settings,
            log=context.log,
        )

        dataset_id = _choose_dataset_id(validated_manifest)
        asset_info = _export_duckdb_join_to_datalake(
            minio=context.resources.minio,
            mongodb=context.resources.mongodb,
            manifest=validated_manifest.model_dump(mode="json"),
            output_path=join_result["output_path"],
            spatial_metadata_path=str(spatial_schema_path),
            crs=str(spatial_asset.crs),
            bounds_dict=join_result["bounds"],
            geometry_type=join_result.get("geometry_type"),
            dataset_id=dataset_id,
            dagster_run_id=dagster_run_id,
            run_id=run_id,
            log=context.log,
        )

    joined_asset_id = asset_info["asset_id"]
    run_id = asset_info["run_id"]  # MongoDB ObjectId string

    # spatial_object_id and tabular_object_id are MongoDB ObjectId strings from _resolve_join_assets
    spatial_lineage_id = context.resources.mongodb.insert_lineage(
        source_asset_id=spatial_object_id,
        target_asset_id=joined_asset_id,
        run_id=run_id,
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
        run_id=run_id,
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
            "spatial_parent": MetadataValue.text(
                f"{spatial_asset.dataset_id}@v{spatial_asset.version}"
            ),
            "tabular_parent": MetadataValue.text(
                f"{tabular_asset.dataset_id}@v{tabular_asset.version}"
            ),
            "join_type": MetadataValue.text(join_config.how),
        }
    )

    return asset_info
