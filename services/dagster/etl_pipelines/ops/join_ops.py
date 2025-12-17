# =============================================================================
# Join Operations - Dataset Join Logic
# =============================================================================
# Core operations for joining tabular and spatial datasets in PostGIS
# =============================================================================

import tempfile
import uuid
from pathlib import Path
from typing import Dict, Any

import pyarrow.parquet as pq
from dagster import op, OpExecutionContext, In, Out

from libs.models import Manifest, JoinConfig, AssetKind
from libs.spatial_utils import RunIdSchemaMapping
from ..resources.gdal_resource import GDALResult
from ..resources import MinIOResource, PostGISResource, GDALResource, MongoDBResource


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
        - secondary_asset: Asset model of spatial parent
        - join_config: JoinConfig from manifest

    Raises:
        ValueError: If target_asset_id is None or asset not found
    """
    from libs.models import Manifest, JoinConfig, AssetKind

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


def _load_tabular_to_postgis_from_file(
    postgis,
    parquet_path: str,
    schema: str,
    table_name: str,
    log,
) -> Dict[str, Any]:
    """
    Load Parquet file into PostGIS as a regular table.

    Uses PyArrow to read Parquet and pandas to_sql for efficient bulk insert.

    Args:
        postgis: PostGISResource
        parquet_path: Path to Parquet file
        schema: Target PostGIS schema
        table_name: Target table name
        log: Logger

    Returns:
        Dict with schema, table, columns, row_count
    """
    import pandas as pd

    # Read Parquet with PyArrow
    table = pq.read_table(parquet_path)
    columns = table.column_names
    log.info(f"Read Parquet: {len(table)} rows, {len(columns)} columns")

    # Create table in PostGIS
    # Generate CREATE TABLE from Arrow schema
    type_map = {
        "int64": "BIGINT",
        "int32": "INTEGER",
        "float64": "DOUBLE PRECISION",
        "float32": "REAL",
        "string": "TEXT",
        "large_string": "TEXT",
        "bool": "BOOLEAN",
        "date32": "DATE",
        "timestamp[us]": "TIMESTAMP",
    }

    col_defs = []
    for field in table.schema:
        pg_type = type_map.get(str(field.type), "TEXT")
        col_defs.append(f'"{field.name}" {pg_type}')

    create_sql = f'CREATE TABLE "{schema}"."{table_name}" ({", ".join(col_defs)});'
    postgis.execute_sql(create_sql, schema)
    log.info(f"Created table {schema}.{table_name}")

    # Bulk insert using pandas to_sql
    df = table.to_pandas()
    engine = postgis.get_engine()
    df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists="append",
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
        Dict with schema, table, geom_column, crs
    """
    from libs.s3_utils import s3_to_vsis3

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
        Dict compatible with export_to_datalake (schema, table, bounds, geom_column, manifest)
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
