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
    Manifest,
    OutputFormat,
)
from libs.normalization import extract_column_schema
from libs.s3_utils import s3_to_vsis3
from .duckdb_settings import (
    DEFAULT_DUCKDB_MEMORY_LIMIT,
    DEFAULT_DUCKDB_TEMP_SUBDIR,
    DuckDBJoinSettings,
)

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

    # Warn about potential column name collision
    # The join SELECT uses t.* and adds s.geom. If tabular already has a 'geom' column,
    # PostgreSQL will not raise an error but the output will have ambiguous meaning.
    log.warning(
        "Join output uses 't.*' from tabular + 's.geom' from spatial. "
        "If the tabular asset already contains a 'geom' column, consider renaming it before joining."
    )

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


def _parse_duckdb_extent(extent_value: Any) -> dict[str, float] | None:
    if extent_value is None:
        return None

    if isinstance(extent_value, dict):
        return {
            "minx": float(extent_value["minx"]),
            "miny": float(extent_value["miny"]),
            "maxx": float(extent_value["maxx"]),
            "maxy": float(extent_value["maxy"]),
        }

    for attribute in ("minx", "miny", "maxx", "maxy"):
        if not hasattr(extent_value, attribute):
            break
    else:
        return {
            "minx": float(extent_value.minx),
            "miny": float(extent_value.miny),
            "maxx": float(extent_value.maxx),
            "maxy": float(extent_value.maxy),
        }

    match = re.match(
        r"BOX\(([-0-9.eE]+) ([-0-9.eE]+),([-0-9.eE]+) ([-0-9.eE]+)\)",
        str(extent_value),
    )
    if match is None:
        return None

    minx, miny, maxx, maxy = map(float, match.groups())
    return {"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy}


def _execute_duckdb_join(
    *,
    tabular_path: str,
    spatial_path: str,
    left_key: str,
    right_key: str,
    how: Literal["left", "inner", "right", "outer"],
    output_path: str,
    temp_dir: str,
    memory_limit: str | None = None,
    s3_settings: DuckDBJoinSettings | None = None,
    log=None,
) -> Dict[str, Any]:
    """
    Execute a DuckDB join between tabular and spatial Parquet sources.

    Uses t.* from the tabular source plus s.geom from the spatial source to
    standardize geometry output.
    """

    _require_identifier(left_key, label="left_key")
    _require_identifier(right_key, label="right_key")

    join_type_map = {
        "left": "LEFT JOIN",
        "inner": "INNER JOIN",
        "right": "RIGHT JOIN",
        "outer": "FULL OUTER JOIN",
    }
    join_clause = join_type_map[how]

    temp_dir_path = Path(temp_dir) / DEFAULT_DUCKDB_TEMP_SUBDIR
    temp_dir_path.mkdir(parents=True, exist_ok=True)
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    memory_limit_value = memory_limit or DEFAULT_DUCKDB_MEMORY_LIMIT

    import duckdb

    con = duckdb.connect(database=":memory:")
    try:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

        con.execute(f"PRAGMA temp_directory='{temp_dir_path}';")
        con.execute(f"PRAGMA memory_limit='{memory_limit_value}';")

        if s3_settings is not None:
            con.execute(f"SET s3_endpoint='{s3_settings.s3_endpoint}';")
            con.execute(
                f"SET s3_access_key_id='{s3_settings.s3_access_key_id}';"
            )
            con.execute(
                f"SET s3_secret_access_key='{s3_settings.s3_secret_access_key}';"
            )
            con.execute(f"SET s3_url_style='{s3_settings.s3_url_style}';")
            con.execute(f"SET s3_use_ssl={str(s3_settings.s3_use_ssl).lower()};")

        if log is not None:
            log.info("Executing DuckDB join for join_asset")

        join_sql = f"""
        CREATE OR REPLACE TABLE joined AS
        SELECT
            t.*,
            s.geom AS geom
        FROM read_parquet('{tabular_path}') t
        {join_clause} read_parquet('{spatial_path}') s
        ON CAST(t."{left_key}" AS VARCHAR) = CAST(s."{right_key}" AS VARCHAR);
        """
        con.execute(join_sql)

        row_count = con.execute("SELECT COUNT(*) FROM joined").fetchone()[0]

        geometry_row = con.execute(
            "SELECT ST_GeometryType(geom) FROM joined WHERE geom IS NOT NULL LIMIT 1"
        ).fetchone()
        geometry_type = "UNKNOWN"
        if geometry_row and geometry_row[0]:
            geometry_type = geometry_row[0]

        extent_row = con.execute(
            "SELECT ST_Extent(geom) FROM joined WHERE geom IS NOT NULL"
        ).fetchone()
        bounds = _parse_duckdb_extent(extent_row[0] if extent_row else None)

        con.execute(f"COPY joined TO '{output_path_obj}' (FORMAT GEOPARQUET)")

        return {
            "output_path": str(output_path_obj),
            "row_count": row_count,
            "bounds": bounds,
            "geometry_type": geometry_type,
        }
    finally:
        con.close()


def _rewrite_parquet_with_metadata(
    *,
    input_path: str,
    output_path: str,
    metadata: dict[bytes, bytes],
) -> None:
    parquet_file = pq.ParquetFile(input_path)
    schema = parquet_file.schema_arrow.with_metadata(metadata)
    writer = pq.ParquetWriter(output_path, schema)
    try:
        for batch in parquet_file.iter_batches():
            writer.write_table(pa.Table.from_batches([batch], schema=schema))
    finally:
        writer.close()


def _merge_geoparquet_metadata(
    *,
    source_path: str,
    target_path: str,
    log=None,
) -> bool:
    source_metadata = pq.ParquetFile(source_path).metadata.metadata or {}
    target_metadata = pq.ParquetFile(target_path).metadata.metadata or {}

    if b"geo" in target_metadata:
        return False
    if b"geo" not in source_metadata:
        if log is not None:
            log.warning("GeoParquet metadata missing from spatial source")
        return False

    merged_metadata = dict(target_metadata)
    merged_metadata[b"geo"] = source_metadata[b"geo"]

    temp_path = f"{target_path}.tmp"
    _rewrite_parquet_with_metadata(
        input_path=target_path,
        output_path=temp_path,
        metadata=merged_metadata,
    )
    Path(temp_path).replace(target_path)

    if log is not None:
        log.info("Merged GeoParquet metadata into join output")
    return True


def _export_duckdb_join_to_datalake(
    *,
    minio,
    mongodb,
    manifest: Dict[str, Any],
    output_path: str,
    spatial_metadata_path: str | None,
    crs: str,
    bounds_dict: dict[str, float] | None,
    geometry_type: str | None,
    dataset_id: str,
    dagster_run_id: str,
    run_id: str,
    log,
) -> Dict[str, Any]:
    dataset_id = dataset_id.strip()
    if not dataset_id:
        raise ValueError("dataset_id must be a non-empty string")

    version = mongodb.get_next_version(dataset_id)
    s3_key = f"{dataset_id}/v{version}/data.parquet"
    log.info(f"Export target: {s3_key}")

    if spatial_metadata_path is not None:
        _merge_geoparquet_metadata(
            source_path=spatial_metadata_path,
            target_path=output_path,
            log=log,
        )

    sha256_hash = hashlib.sha256()
    with open(output_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    content_hash = f"sha256:{sha256_hash.hexdigest()}"

    minio.upload_to_lake(output_path, s3_key)
    log.info(f"Uploaded joined output to data lake: {s3_key}")

    log.info("Extracting column schema from joined GeoParquet")
    parquet_schema = pq.read_schema(output_path)
    column_schema = extract_column_schema(parquet_schema)
    log.info(f"Captured column schema with {len(column_schema)} columns")

    validated_manifest = Manifest(**manifest)
    asset_metadata = AssetMetadata.from_manifest_metadata(
        validated_manifest.metadata,
        geometry_type=geometry_type,
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
        crs=crs,
        bounds=bounds,
        metadata=asset_metadata,
        created_at=datetime.now(timezone.utc),
        updated_at=None,
    )

    inserted_id = mongodb.insert_asset(asset)
    log.info(f"Registered joined asset in MongoDB: {inserted_id}")

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
            crs=crs,
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
