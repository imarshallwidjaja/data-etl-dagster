"""
Join ops - core logic for derived join assets (Phase 4).

Implements a tabular (primary/left) + spatial (secondary/right) join that produces
a spatialized tabular GeoParquet output in the data lake and records lineage in MongoDB.
"""

from __future__ import annotations

from pathlib import Path

import hashlib
import re
import tempfile
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Literal

import pyarrow as pa

import pyarrow.parquet as pq
from libs.models import (
    Asset,
    AssetKind,
    AssetMetadata,
    Bounds,
    CRS,
    Manifest,
    OutputFormat,
)
from libs.normalization import extract_column_schema
from libs.s3_utils import s3_to_vsis3

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _require_identifier(name: str, *, label: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid {label}: {name!r}. Must match: {_IDENTIFIER_RE.pattern}"
        )
    return name


def _resolve_join_assets(
    *,
    mongodb,
    manifest: Dict[str, Any],
    log,
) -> Dict[str, Any]:
    """Resolve and validate join inputs.

    Both spatial_dataset_id and tabular_dataset_id are required.
    Optional version fields allow pinning to specific versions (None = latest).
    Returns both assets fetched from MongoDB along with their ObjectIDs for lineage.
    """
    validated = Manifest(**manifest)
    join_config = validated.metadata.join_config
    if join_config is None:
        raise ValueError(
            f"Manifest {validated.batch_id} has intent 'join_datasets' but metadata.join_config is missing"
        )

    # Resolve spatial asset by dataset_id + optional version
    if join_config.spatial_version is not None:
        spatial_asset = mongodb.get_asset(
            join_config.spatial_dataset_id, join_config.spatial_version
        )
        spatial_version_info = f"v{join_config.spatial_version} (pinned)"
    else:
        spatial_asset = mongodb.get_latest_asset(join_config.spatial_dataset_id)
        spatial_version_info = (
            f"v{spatial_asset.version} (latest)" if spatial_asset else "N/A"
        )

    if spatial_asset is None:
        version_suffix = (
            f" v{join_config.spatial_version}"
            if join_config.spatial_version
            else " (latest)"
        )
        raise ValueError(
            f"Spatial dataset not found: {join_config.spatial_dataset_id}{version_suffix}"
        )
    if spatial_asset.kind != AssetKind.SPATIAL:
        raise ValueError(
            f"spatial_dataset_id must reference a spatial asset, "
            f"got {spatial_asset.kind.value} for '{join_config.spatial_dataset_id}'"
        )

    # Get MongoDB ObjectID for lineage recording
    spatial_object_id = mongodb.get_asset_object_id_for_version(
        join_config.spatial_dataset_id, spatial_asset.version
    )

    # Resolve tabular asset by dataset_id + optional version
    if join_config.tabular_version is not None:
        tabular_asset = mongodb.get_asset(
            join_config.tabular_dataset_id, join_config.tabular_version
        )
        tabular_version_info = f"v{join_config.tabular_version} (pinned)"
    else:
        tabular_asset = mongodb.get_latest_asset(join_config.tabular_dataset_id)
        tabular_version_info = (
            f"v{tabular_asset.version} (latest)" if tabular_asset else "N/A"
        )

    if tabular_asset is None:
        version_suffix = (
            f" v{join_config.tabular_version}"
            if join_config.tabular_version
            else " (latest)"
        )
        raise ValueError(
            f"Tabular dataset not found: {join_config.tabular_dataset_id}{version_suffix}"
        )
    if tabular_asset.kind != AssetKind.TABULAR:
        raise ValueError(
            f"tabular_dataset_id must reference a tabular asset, "
            f"got {tabular_asset.kind.value} for '{join_config.tabular_dataset_id}'"
        )

    # Get MongoDB ObjectID for lineage recording
    tabular_object_id = mongodb.get_asset_object_id_for_version(
        join_config.tabular_dataset_id, tabular_asset.version
    )

    log.info(
        f"Resolved join inputs: "
        f"spatial={spatial_asset.dataset_id}@{spatial_version_info}, "
        f"tabular={tabular_asset.dataset_id}@{tabular_version_info}"
    )
    log.info(
        f"Join config: LEFT={join_config.left_key}, "
        f"RIGHT={join_config.right_key}, HOW={join_config.how}"
    )

    return {
        "validated_manifest": validated,
        "join_config": join_config,
        "spatial_asset": spatial_asset,
        "spatial_object_id": spatial_object_id,
        "tabular_asset": tabular_asset,
        "tabular_object_id": tabular_object_id,
    }


def _load_tabular_arrow_to_postgis(
    *,
    postgis,
    table: pa.Table,
    schema: str,
    table_name: str,
    log,
) -> Dict[str, Any]:
    """
    Load an in-memory Arrow table into PostGIS as a regular (non-spatial) table.

    Uses pandas+SQLAlchemy for simplicity; PostGIS remains transient compute only.
    """
    _require_identifier(schema, label="schema")
    _require_identifier(table_name, label="table_name")

    df = table.to_pandas()
    engine = postgis.get_engine()
    log.info(f"Loading tabular rows to PostGIS: {schema}.{table_name} ({len(df)} rows)")

    df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=10_000,
    )

    return {
        "schema": schema,
        "table": table_name,
        "columns": list(table.column_names),
        "row_count": len(df),
    }


def _load_geoparquet_to_postgis(
    *,
    gdal,
    postgis,
    minio,
    s3_key: str,
    schema: str,
    table_name: str,
    log,
) -> Dict[str, Any]:
    """
    Load GeoParquet from the data lake to PostGIS using GDAL ogr2ogr (vsis3).
    """
    _require_identifier(schema, label="schema")
    _require_identifier(table_name, label="table_name")

    full_s3_path = f"s3://{minio.lake_bucket}/{s3_key}"
    vsis3_path = s3_to_vsis3(full_s3_path)

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
        raise RuntimeError(f"ogr2ogr failed loading GeoParquet: {result.stderr}")

    log.info(f"Loaded GeoParquet to PostGIS: {schema}.{table_name}")
    return {"schema": schema, "table": table_name, "geom_column": "geom"}


def _load_tabular_parquet_to_postgis(
    *,
    minio,
    postgis,
    s3_key: str,
    schema: str,
    table_name: str,
    log,
) -> Dict[str, Any]:
    """Load tabular Parquet from data-lake to PostGIS as a non-spatial table.

    Downloads parquet to a temp file, reads with pyarrow, loads to PostGIS.
    """
    import tempfile

    import pyarrow.parquet as pq

    _require_identifier(schema, label="schema")
    _require_identifier(table_name, label="table_name")

    log.info(f"Loading tabular parquet from data-lake: {s3_key}")

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        minio.download_from_lake(s3_key, tmp_path)

        table = pq.read_table(tmp_path)
        df = table.to_pandas()

        # Drop geometry columns if present (tabular only)
        if "geometry" in df.columns:
            df = df.drop(columns=["geometry"])
        if "geom" in df.columns:
            df = df.drop(columns=["geom"])

        engine = postgis.get_engine()
        log.info(
            f"Loading tabular parquet to PostGIS: {schema}.{table_name} ({len(df)} rows)"
        )

        df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=10_000,
        )

        return {
            "schema": schema,
            "table": table_name,
            "columns": list(df.columns),
            "row_count": len(df),
        }
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def _execute_spatial_join(
    *,
    postgis,
    schema: str,
    tabular_table: str,
    spatial_table: str,
    left_key: str,
    right_key: str,
    how: Literal["left", "inner", "right", "outer"],
    output_table: str,
    log,
) -> Dict[str, Any]:
    """
    Execute SQL JOIN between tabular and spatial tables.

    Tabular is LEFT, spatial is RIGHT. Geometry comes from spatial and is standardized to `geom`.
    """
    _require_identifier(schema, label="schema")
    _require_identifier(tabular_table, label="tabular_table")
    _require_identifier(spatial_table, label="spatial_table")
    _require_identifier(left_key, label="left_key")
    _require_identifier(right_key, label="right_key")
    _require_identifier(output_table, label="output_table")

    join_type_map = {
        "left": "LEFT JOIN",
        "inner": "INNER JOIN",
        "right": "RIGHT JOIN",
        "outer": "FULL OUTER JOIN",
    }
    join_clause = join_type_map[how]

    sql = f"""
    CREATE TABLE "{schema}"."{output_table}" AS
    SELECT
        t.*,
        s."geom" AS geom
    FROM "{schema}"."{tabular_table}" t
    {join_clause} "{schema}"."{spatial_table}" s
    ON t."{left_key}"::TEXT = s."{right_key}"::TEXT;
    """
    postgis.execute_sql(sql, schema)
    log.info(f"Created joined table: {schema}.{output_table}")

    bounds = postgis.get_table_bounds(schema, output_table, geom_column="geom")
    bounds_dict = None
    if bounds is not None:
        bounds_dict = {
            "minx": bounds.minx,
            "miny": bounds.miny,
            "maxx": bounds.maxx,
            "maxy": bounds.maxy,
        }

    # Extract geometry type for joined output (Milestone 2)
    try:
        geometry_type = postgis.get_geometry_type(
            schema, output_table, geom_column="geom"
        )
        log.info(f"Captured geometry type for joined asset: {geometry_type}")
    except Exception as e:
        log.warning(f"Failed to extract geometry type: {e}. Using UNKNOWN.")
        geometry_type = "UNKNOWN"

    return {
        "schema": schema,
        "table": output_table,
        "bounds": bounds_dict,
        "geom_column": "geom",
        "geometry_type": geometry_type,  # Milestone 2: spatial metadata capture
    }


def _export_joined_to_datalake(
    *,
    gdal,
    postgis,
    minio,
    mongodb,
    schema: str,
    table: str,
    manifest: Dict[str, Any],
    crs: str,
    bounds_dict: dict[str, float] | None,
    geometry_type: str | None,  # Milestone 2: spatial metadata capture
    dataset_id: str,
    dagster_run_id: str,
    run_id: str,
    log,
) -> Dict[str, Any]:
    """
    Export joined table from PostGIS -> GeoParquet in data lake, register asset in MongoDB.
    """
    _require_identifier(schema, label="schema")
    _require_identifier(table, label="table")

    dataset_id = dataset_id.strip()
    if not dataset_id:
        raise ValueError("dataset_id must be a non-empty string")

    version = mongodb.get_next_version(dataset_id)
    s3_key = f"{dataset_id}/v{version}/data.parquet"
    log.info(f"Export target: {s3_key}")

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    temp_file_path = temp_file.name
    temp_file.close()

    try:
        pg_conn_str = (
            f"PG:host={postgis.host} "
            f"dbname={postgis.database} "
            f"schemas={schema} "
            f"user={postgis.user} "
            f"password={postgis.password} "
            f"tables={table}"
        )

        result = gdal.ogr2ogr(
            input_path=pg_conn_str,
            output_path=temp_file_path,
            output_format="Parquet",
            target_crs=crs,
        )
        if not result.success:
            raise RuntimeError(f"ogr2ogr export failed: {result.stderr}")

        sha256_hash = hashlib.sha256()
        with open(temp_file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        content_hash = f"sha256:{sha256_hash.hexdigest()}"

        minio.upload_to_lake(temp_file_path, s3_key)
        log.info(f"Uploaded joined output to data lake: {s3_key}")

        # Extract column schema from joined GeoParquet
        log.info("Extracting column schema from joined GeoParquet")
        parquet_schema = pq.read_schema(temp_file_path)
        column_schema = extract_column_schema(parquet_schema)
        log.info(f"Captured column schema with {len(column_schema)} columns")

        # Create Asset model using factory method for consistent metadata propagation
        validated_manifest = Manifest(**manifest)
        asset_metadata = AssetMetadata.from_manifest_metadata(
            validated_manifest.metadata,
            geometry_type=geometry_type,  # Milestone 2: spatial metadata capture
            column_schema=column_schema,
        )

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
            run_id=run_id,
            kind=AssetKind.JOINED,
            format=OutputFormat.GEOPARQUET,
            crs=CRS(crs),
            bounds=bounds,
            metadata=asset_metadata,
            created_at=datetime.now(timezone.utc),
            updated_at=None,
        )

        inserted_id = mongodb.insert_asset(asset)
        log.info(f"Registered joined asset in MongoDB: {inserted_id}")

        # Link asset to run document
        mongodb.add_asset_to_run(dagster_run_id, inserted_id)
        log.info(f"Linked asset to run: {dagster_run_id}")

        return {
            "asset_id": inserted_id,
            "s3_key": s3_key,
            "dataset_id": dataset_id,
            "version": version,
            "content_hash": content_hash,
            "run_id": run_id,
        }
    finally:
        Path(temp_file_path).unlink(missing_ok=True)


def _choose_dataset_id(manifest: Manifest) -> str:
    """
    Use user-provided metadata.tags.dataset_id with UUID fallback.
    """
    raw = manifest.metadata.tags.get("dataset_id")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return f"dataset_{uuid.uuid4().hex[:12]}"
