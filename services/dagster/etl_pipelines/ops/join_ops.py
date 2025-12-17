"""
Join ops - core logic for derived join assets (Phase 4).

Implements a tabular (primary/left) + spatial (secondary/right) join that produces
a spatialized tabular GeoParquet output in the data lake and records lineage in MongoDB.
"""

from __future__ import annotations

import hashlib
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Literal

import pandas as pd
import pyarrow as pa

from libs.models import Asset, AssetKind, AssetMetadata, Bounds, CRS, JoinConfig, Manifest, OutputFormat
from libs.s3_utils import s3_to_vsis3

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _require_identifier(name: str, *, label: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {label}: {name!r}. Must match: {_IDENTIFIER_RE.pattern}")
    return name


def _resolve_join_assets(
    *,
    mongodb,
    manifest: Dict[str, Any],
    log,
) -> Dict[str, Any]:
    """
    Resolve and validate join inputs.

    - Primary is incoming tabular (from manifest)
    - Secondary is an existing spatial asset (from MongoDB via join_config.target_asset_id)
    """
    validated = Manifest(**manifest)
    join_config = validated.metadata.join_config
    if join_config is None:
        raise ValueError(
            f"Manifest {validated.batch_id} has intent 'join_datasets' but metadata.join_config is missing"
        )
    if join_config.target_asset_id is None:
        raise ValueError(
            f"Manifest {validated.batch_id} requires join_config.target_asset_id for join_datasets"
        )

    secondary_asset = mongodb.get_asset_by_id(join_config.target_asset_id)
    if secondary_asset is None:
        raise ValueError(f"Secondary asset not found: {join_config.target_asset_id}")
    if secondary_asset.kind != AssetKind.SPATIAL:
        raise ValueError(f"Secondary asset must be spatial, got {secondary_asset.kind.value}")

    log.info(f"Resolved secondary asset dataset_id={secondary_asset.dataset_id} v{secondary_asset.version}")
    log.info(
        f"Join config: LEFT={join_config.left_key}, RIGHT={join_config.right_key or join_config.left_key}, HOW={join_config.how}"
    )

    return {
        "validated_manifest": validated,
        "join_config": join_config,
        "secondary_asset": secondary_asset,
        "secondary_asset_id": join_config.target_asset_id,
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
    ON t."{left_key}" = s."{right_key}";
    """
    postgis.execute_sql(sql, schema)
    log.info(f"Created joined table: {schema}.{output_table}")

    bounds = postgis.get_table_bounds(schema, output_table, geom_column="geom")
    bounds_dict = None
    if bounds is not None:
        bounds_dict = {"minx": bounds.minx, "miny": bounds.miny, "maxx": bounds.maxx, "maxy": bounds.maxy}

    return {"schema": schema, "table": output_table, "bounds": bounds_dict, "geom_column": "geom"}


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
    dataset_id: str,
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

        manifest_tags = manifest["metadata"].get("tags", {})
        asset_metadata = AssetMetadata(
            title=manifest["metadata"].get("project", dataset_id),
            description=manifest["metadata"].get("description"),
            source=None,
            license=None,
            tags=manifest_tags,
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
            dagster_run_id=run_id,
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


