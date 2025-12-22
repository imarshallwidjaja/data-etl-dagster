# =============================================================================
# Tabular Ops - CSV to Parquet Pipeline
# =============================================================================
# Operations for ingesting tabular data (CSV) without PostGIS.
# Direct CSV → Parquet conversion with header cleaning.
# =============================================================================

import hashlib
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.parquet as pq

from dagster import op, OpExecutionContext, In, Out

from libs.models import (
    Asset,
    AssetKind,
    AssetMetadata,
    OutputFormat,
    Manifest,
    ColumnInfo,
)
from libs.s3_utils import extract_s3_key
from libs.spatial_utils import normalize_headers
from libs.normalization import normalize_arrow_schema


def _download_tabular_from_landing(
    minio,
    manifest: Dict[str, Any],
    log,
) -> Dict[str, Any]:
    """
    Core logic for downloading tabular file from landing zone.

    This function is extracted for easier unit testing and asset usage.

    Args:
        minio: MinIOResource instance
        manifest: Manifest dict (validated)
        log: Logger instance

    Returns:
        Dict with local_file_path and manifest

    Raises:
        ValueError: If manifest has multiple files
        RuntimeError: If download fails
    """
    validated_manifest = Manifest(**manifest)

    # Phase 3: Tabular manifests are single-file only
    if len(validated_manifest.files) != 1:
        raise ValueError(
            f"Tabular ingestion requires exactly one file, got {len(validated_manifest.files)}"
        )

    file_entry = validated_manifest.files[0]
    s3_path = file_entry.path

    # Extract S3 key from path
    s3_key = extract_s3_key(s3_path)

    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    temp_file_path = temp_file.name
    temp_file.close()

    try:
        log.info(f"Downloading tabular file from landing zone: {s3_key}")
        minio.download_from_landing(s3_key, temp_file_path)
        log.info(f"Downloaded to temporary file: {temp_file_path}")

        return {
            "local_file_path": temp_file_path,
            "manifest": manifest,
        }

    except Exception as e:
        # Clean up temp file on error
        try:
            Path(temp_file_path).unlink(missing_ok=True)
        except Exception:
            pass
        raise RuntimeError(f"Failed to download tabular file '{s3_key}': {e}") from e


def _load_and_clean_tabular(
    download_result: Dict[str, Any],
    log,
) -> Dict[str, Any]:
    """
    Core logic for loading CSV and cleaning headers.

    This function is extracted for easier unit testing and asset usage.

    Args:
        download_result: Dict with local_file_path and manifest
        log: Logger instance

    Returns:
        Dict with table, header_mapping, row_count, columns, join_key_clean, manifest

    Raises:
        RuntimeError: If CSV read or header cleaning fails
        ValueError: If join_key is required but missing
    """
    local_file_path = download_result["local_file_path"]
    manifest = download_result["manifest"]
    validated_manifest = Manifest(**manifest)

    try:
        log.info(f"Reading CSV file: {local_file_path}")

        # Read CSV into Arrow Table
        table = csv.read_csv(
            local_file_path,
            parse_options=csv.ParseOptions(delimiter=","),
            read_options=csv.ReadOptions(use_threads=True),
        )

        original_headers = table.column_names
        log.info(f"Read {len(original_headers)} columns, {len(table)} rows")

        # Apply header cleaning
        header_mapping, cleaned_headers = normalize_headers(original_headers)
        log.info(f"Header mapping: {header_mapping}")

        # Rename columns in table
        table = table.rename_columns(cleaned_headers)

        # Handle join_key if provided
        join_key_clean = None
        join_config = validated_manifest.metadata.join_config
        if join_config:
            left_key = join_config.left_key
            if left_key in header_mapping:
                join_key_clean = header_mapping[left_key]
                log.info(f"Join key '{left_key}' mapped to '{join_key_clean}'")

                if join_key_clean not in cleaned_headers:
                    raise ValueError(
                        f"Join key '{left_key}' (cleaned: '{join_key_clean}') not found in cleaned headers"
                    )

                # Normalize join key column to string type and trim whitespace
                col_idx = cleaned_headers.index(join_key_clean)
                col = table.column(join_key_clean).cast(pa.string())
                col = pc.utf8_trim_whitespace(col)
                table = table.set_column(col_idx, join_key_clean, col)
                log.info(
                    f"Normalized join key column '{join_key_clean}' to string (trimmed whitespace)"
                )
            else:
                raise ValueError(
                    f"Join key '{left_key}' not found in original headers: {original_headers}"
                )

        row_count = len(table)
        log.info(f"Processed table: {row_count} rows, {len(cleaned_headers)} columns")

        return {
            "table": table,
            "header_mapping": header_mapping,
            "row_count": row_count,
            "columns": cleaned_headers,
            "join_key_clean": join_key_clean,
            "manifest": manifest,
        }

    except Exception as e:
        raise RuntimeError(f"Failed to load and clean tabular data: {e}") from e
    finally:
        # Clean up local file
        try:
            Path(local_file_path).unlink(missing_ok=True)
            log.info(f"Cleaned up temporary file: {local_file_path}")
        except Exception as cleanup_error:
            log.warning(f"Failed to clean up temporary file: {cleanup_error}")


def _export_tabular_parquet_to_datalake(
    minio,
    mongodb,
    table_info: Dict[str, Any],
    dagster_run_id: str,
    run_id: str,
    log,
) -> Dict[str, Any]:
    """
    Core logic for exporting tabular data to data lake.

    This function is extracted for easier unit testing and asset usage.

    Args:
        minio: MinIOResource instance
        mongodb: MongoDBResource instance
        table_info: Table info dict from _load_and_clean_tabular
        dagster_run_id: Dagster run ID (for asset linking)
        run_id: MongoDB run document ObjectId
        log: Logger instance

    Returns:
        Asset info dict with asset_id, s3_key, dataset_id, version, content_hash, run_id

    Raises:
        RuntimeError: If Parquet write, upload, or MongoDB insert fails
    """
    manifest = table_info["manifest"]
    validated_manifest = Manifest(**manifest)
    table = table_info["table"]
    header_mapping = table_info["header_mapping"]
    join_key_clean = table_info.get("join_key_clean")

    # Generate dataset_id
    dataset_id = validated_manifest.metadata.tags.get("dataset_id")
    if not dataset_id:
        dataset_id = f"dataset_{uuid.uuid4().hex[:12]}"
    else:
        if not isinstance(dataset_id, str):
            raise ValueError(
                f"metadata.tags.dataset_id must be a string, got {type(dataset_id).__name__}"
            )
    log.info(f"Using dataset_id: {dataset_id}")

    # Get next version number
    version = mongodb.get_next_version(dataset_id)
    log.info(f"Dataset version: {version}")

    # Generate S3 key
    s3_key = f"{dataset_id}/v{version}/data.parquet"
    log.info(f"Target S3 key: {s3_key}")

    # Create temporary file for Parquet
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    temp_file_path = temp_file.name
    temp_file.close()

    try:
        # Write Arrow Table to Parquet
        log.info(f"Writing Parquet file: {temp_file_path}")
        pq.write_table(table, temp_file_path)
        log.info("Successfully wrote Parquet file")

        # Calculate SHA256 content hash
        sha256_hash = hashlib.sha256()
        with open(temp_file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        content_hash = f"sha256:{sha256_hash.hexdigest()}"
        log.info(f"Calculated content hash: {content_hash[:20]}...")

        # Upload to MinIO data lake
        minio.upload_to_lake(temp_file_path, s3_key)
        log.info(f"Uploaded to MinIO data lake: {s3_key}")

        # Create Asset model using factory method for consistent metadata propagation
        manifest_tags = validated_manifest.metadata.tags.copy()
        if join_key_clean:
            manifest_tags["join_key_clean"] = join_key_clean

        # Build column_schema from Arrow table schema
        normalized_schema = normalize_arrow_schema(table.schema)
        column_schema = {}
        for field_name, normalized in normalized_schema.items():
            column_schema[field_name] = ColumnInfo(
                title=field_name,
                description="",  # Empty for now
                type_name=normalized["type_name"],
                logical_type=normalized["logical_type"],
                nullable=normalized["nullable"],
            )
        log.info(f"Captured column schema with {len(column_schema)} columns")

        asset_metadata = AssetMetadata.from_manifest_metadata(
            validated_manifest.metadata,
            header_mapping=header_mapping,
            column_schema=column_schema,
        )
        # Add the join_key_clean to tags if present (post-factory adjustment)
        if join_key_clean:
            asset_metadata.tags["join_key_clean"] = join_key_clean

        asset = Asset(
            s3_key=s3_key,
            dataset_id=dataset_id,
            version=version,
            content_hash=content_hash,
            run_id=run_id,
            kind=AssetKind.TABULAR,
            format=OutputFormat.PARQUET,
            crs=None,
            bounds=None,
            metadata=asset_metadata,
            created_at=datetime.now(timezone.utc),
            updated_at=None,
        )

        # Register in MongoDB
        inserted_id = mongodb.insert_asset(asset)
        log.info(f"Registered asset in MongoDB: {inserted_id}")

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
        # Clean up temporary file
        try:
            Path(temp_file_path).unlink(missing_ok=True)
            log.info(f"Cleaned up temporary file: {temp_file_path}")
        except Exception as e:
            log.warning(f"Failed to clean up temporary file {temp_file_path}: {e}")


@op(
    ins={"manifest": In(dagster_type=dict)},
    out={"download_result": Out(dagster_type=dict)},
    required_resource_keys={"minio"},
)
def download_tabular_from_landing(context: OpExecutionContext, manifest: dict) -> dict:
    """
    Download tabular file from MinIO landing zone to local temp file.

    Args:
        context: Dagster op execution context
        manifest: Manifest dict (validated)

    Returns:
        Dict with local_file_path and manifest

    Raises:
        ValueError: If manifest has multiple files (tabular is single-file only)
        RuntimeError: If download fails
    """
    return _download_tabular_from_landing(
        minio=context.resources.minio,
        manifest=manifest,
        log=context.log,
    )


@op(
    ins={"download_result": In(dagster_type=dict)},
    out={"table_info": Out(dagster_type=dict)},
)
def load_and_clean_tabular(
    context: OpExecutionContext,
    download_result: dict,
) -> dict:
    """
    Load CSV into Arrow Table and apply header cleaning.

    Args:
        context: Dagster op execution context
        download_result: Dict with local_file_path and manifest

    Returns:
        Dict containing:
        - table: Arrow Table
        - header_mapping: Dict mapping original → cleaned headers
        - row_count: Number of rows
        - columns: List of cleaned column names
        - join_key_clean: Cleaned join key column name (if join_key provided)
        - manifest: Manifest dict

    Raises:
        RuntimeError: If CSV read or header cleaning fails
        ValueError: If join_key is required but missing
    """
    return _load_and_clean_tabular(
        download_result=download_result,
        log=context.log,
    )


@op(
    ins={"table_info": In(dagster_type=dict)},
    out={"asset_info": Out(dagster_type=dict)},
    required_resource_keys={"minio", "mongodb"},
)
def export_tabular_parquet_to_datalake(
    context: OpExecutionContext,
    table_info: dict,
) -> dict:
    """
    Export Arrow Table to Parquet, upload to data lake, and register in MongoDB.

    Args:
        context: Dagster op execution context
        table_info: Table info dict from load_and_clean_tabular

    Returns:
        Asset info dict containing:
        - asset_id: MongoDB ObjectId as string
        - s3_key: S3 object key in data lake
        - dataset_id: Dataset identifier
        - version: Asset version number
        - content_hash: SHA256 content hash
        - run_id: Dagster run ID

    Raises:
        RuntimeError: If Parquet write, upload, or MongoDB insert fails
    """
    # Get MongoDB run ObjectId
    run_id = context.resources.mongodb.get_run_object_id(context.run_id)
    if not run_id:
        raise RuntimeError(f"Run document not found for {context.run_id}")

    return _export_tabular_parquet_to_datalake(
        minio=context.resources.minio,
        mongodb=context.resources.mongodb,
        table_info=table_info,
        dagster_run_id=context.run_id,
        run_id=run_id,
        log=context.log,
    )
