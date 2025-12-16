# =============================================================================
# Raw Assets - Graph-Backed Ingestion Assets
# =============================================================================
# Graph-backed assets for spatial and tabular data ingestion.
# =============================================================================

from dagster import asset, AssetExecutionContext, Field

from libs.models import Manifest

from ..ops import (
    load_to_postgis,
    spatial_transform,
    export_to_datalake,
    download_tabular_from_landing,
    load_and_clean_tabular,
    export_tabular_parquet_to_datalake,
)


@asset(
    name="raw_manifest_json",
    description="Validated manifest JSON from run configuration",
    config_schema={"manifest": Field(dict)},
)
def raw_manifest_json(context: AssetExecutionContext) -> dict:
    """
    Root asset that validates and provides manifest JSON.

    This asset serves as the anchor for ingestion pipelines,
    validating the manifest from run config and providing
    normalized manifest data to downstream assets.

    Args:
        context: Asset execution context with manifest config

    Returns:
        Validated and normalized manifest dict

    Raises:
        ValidationError: If manifest is invalid
    """
    # Validate manifest using Pydantic model
    manifest_config = context.op_config
    validated_manifest = Manifest(**manifest_config["manifest"])

    # Return normalized dict
    return validated_manifest.model_dump(mode="json")


@asset(
    name="raw_spatial_asset",
    description="Processed spatial dataset from raw files to data lake",
    config_schema={"manifest": Field(dict)},
)
def raw_spatial_asset(context: AssetExecutionContext, raw_manifest_json: dict) -> dict:
    """
    Graph-backed asset for spatial data ingestion.

    Wraps the existing spatial ops chain:
    load_to_postgis → spatial_transform → export_to_datalake

    Args:
        context: Asset execution context
        raw_manifest_json: Validated manifest from raw_manifest_json asset

    Returns:
        Asset info dict from export_to_datalake op
    """
    # Load to PostGIS
    schema_info = load_to_postgis(raw_manifest_json)

    # Transform spatial data
    transform_result = spatial_transform(schema_info)

    # Export to data lake and register
    asset_info = export_to_datalake(transform_result)

    return asset_info


@asset(
    name="raw_tabular_asset",
    description="Processed tabular dataset from CSV to Parquet in data lake",
    config_schema={"manifest": Field(dict)},
)
def raw_tabular_asset(context: AssetExecutionContext, raw_manifest_json: dict) -> dict:
    """
    Graph-backed asset for tabular data ingestion.

    Wraps the tabular ops chain:
    download_tabular_from_landing → load_and_clean_tabular → export_tabular_parquet_to_datalake

    Args:
        context: Asset execution context
        raw_manifest_json: Validated manifest from raw_manifest_json asset

    Returns:
        Asset info dict from export_tabular_parquet_to_datalake op
    """
    # Download CSV from landing zone
    local_path = download_tabular_from_landing(raw_manifest_json)

    # Load and clean tabular data
    tabular_data = load_and_clean_tabular(local_path, raw_manifest_json)

    # Export to Parquet in data lake and register
    asset_info = export_tabular_parquet_to_datalake(tabular_data, raw_manifest_json)

    return asset_info
