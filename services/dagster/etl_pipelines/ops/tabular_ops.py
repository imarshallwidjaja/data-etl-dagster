# =============================================================================
# Tabular Ops - CSV to Parquet Pipeline
# =============================================================================
# Ops for ingesting tabular data: download from landing zone, clean headers,
# convert to Parquet, and register in data lake.
# =============================================================================

import hashlib
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

import pyarrow as pa
import pyarrow.csv as pacsv

from dagster import op, OpExecutionContext, In, Out

from libs.models import Asset, AssetMetadata, OutputFormat, AssetKind
from libs.spatial_utils import normalize_tabular_headers


def _download_tabular_from_landing(
    minio,
    s3_key: str,
    local_path: str,
    log,
) -> None:
    """
    Download tabular file from MinIO landing zone to local path.

    Args:
        minio: MinIOResource instance
        s3_key: S3 key in landing zone (e.g., "batch_001/data.csv")
        local_path: Local file path to download to
        log: Logger instance

    Raises:
        RuntimeError: If download fails
    """
    try:
        log.info(f"Downloading tabular file from landing zone: {s3_key}")
        minio.download_from_landing(s3_key, local_path)
        log.info(f"Successfully downloaded to: {local_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to download tabular file {s3_key}: {e}")


def _load_and_clean_tabular(
    local_path: str,
    manifest: Dict[str, Any],
    log,
) -> Dict[str, Any]:
    """
    Load CSV file and apply header cleaning.

    Args:
        local_path: Path to local CSV file
        manifest: Manifest dict for metadata
        log: Logger instance

    Returns:
        Dict containing:
        - table: Arrow Table with cleaned headers
        - header_mapping: Dict mapping original to cleaned headers
        - row_count: Number of rows in table
        - columns: List of cleaned column names

    Raises:
        RuntimeError: If CSV reading or header cleaning fails
    """
    try:
        log.info(f"Loading CSV from: {local_path}")

        # Read CSV with PyArrow
        # Let PyArrow auto-detect delimiter, encoding, etc.
        table = pacsv.read_csv(local_path)

        log.info(f"Loaded table with {table.num_rows} rows and {table.num_columns} columns")

        # Get original headers
        original_headers = table.column_names
        log.info(f"Original headers: {original_headers}")

        # Apply header cleaning
        header_mapping = normalize_tabular_headers(original_headers)
        cleaned_headers = [header_mapping[orig] for orig in original_headers]

        log.info(f"Cleaned headers: {cleaned_headers}")

        # Rename columns in the table
        table = table.rename_columns(cleaned_headers)

        # Validate join key if specified in manifest
        join_config = manifest.get("metadata", {}).get("join_config")
        if join_config and "left_key" in join_config:
            join_key = join_config["left_key"]
            if join_key not in original_headers:
                raise RuntimeError(f"Join key '{join_key}' not found in CSV headers: {original_headers}")

            # Map to cleaned join key
            cleaned_join_key = header_mapping[join_key]
            log.info(f"Validated join key: '{join_key}' -> '{cleaned_join_key}'")

            # Ensure join key column is string type for reliable joins
            join_key_index = cleaned_headers.index(cleaned_join_key)
            if table.schema[join_key_index].id != pa.string().id:
                log.info(f"Converting join key column '{cleaned_join_key}' to string type")
                # Convert to string
                string_column = pa.compute.cast(table.column(join_key_index), pa.string())
                table = table.set_column(join_key_index, cleaned_join_key, string_column)

        return {
            "table": table,
            "header_mapping": header_mapping,
            "row_count": table.num_rows,
            "columns": cleaned_headers,
        }

    except Exception as e:
        raise RuntimeError(f"Failed to load and clean tabular data: {e}")


def _export_tabular_parquet_to_datalake(
    table: pa.Table,
    header_mapping: Dict[str, str],
    minio,
    mongodb,
    manifest: Dict[str, Any],
    run_id: str,
    log,
) -> Dict[str, Any]:
    """
    Export Arrow Table to Parquet and register in data lake.

    Args:
        table: Arrow Table with cleaned data
        header_mapping: Dict mapping original to cleaned headers
        minio: MinIOResource instance
        mongodb: MongoDBResource instance
        manifest: Manifest dict for metadata
        run_id: Dagster run ID
        log: Logger instance

    Returns:
        Dict with asset info (asset_id, s3_key, dataset_id, version, content_hash, run_id)

    Raises:
        RuntimeError: If Parquet writing, upload, or MongoDB insert fails
    """
    # Generate dataset_id
    dataset_id = manifest.get("metadata", {}).get("tags", {}).get("dataset_id")
    if not dataset_id:
        dataset_id = f"dataset_{uuid.uuid4().hex[:12]}"
    log.info(f"Using dataset_id: {dataset_id}")

    # Get next version number
    version = mongodb.get_next_version(dataset_id)
    log.info(f"Dataset version: {version}")

    # Generate S3 key
    s3_key = f"{dataset_id}/v{version}/data.parquet"
    log.info(f"Target S3 key: {s3_key}")

    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    temp_file_path = temp_file.name
    temp_file.close()

    try:
        # Write Parquet to temp file
        log.info(f"Writing Parquet to temporary file: {temp_file_path}")
        pa.parquet.write_table(table, temp_file_path)
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

        # Create Asset model
        asset_metadata = AssetMetadata(
            title=manifest["metadata"].get("project", dataset_id),
            description=manifest["metadata"].get("description"),
            source=None,
            license=None,
            tags=manifest["metadata"].get("tags", {}),
            header_mapping=header_mapping,
        )

        asset = Asset(
            s3_key=s3_key,
            dataset_id=dataset_id,
            version=version,
            content_hash=content_hash,
            dagster_run_id=run_id,
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
        log.info(f"Registered tabular asset in MongoDB: {inserted_id}")

        # Return asset info
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
    out={"local_path": Out(dagster_type=str)},
    required_resource_keys={"minio"},
)
def download_tabular_from_landing(context: OpExecutionContext, manifest: dict) -> str:
    """
    Download tabular file from MinIO landing zone to local temporary file.

    Args:
        context: Dagster op execution context
        manifest: Manifest dict

    Returns:
        Local path to downloaded file

    Raises:
        RuntimeError: If download fails or manifest has multiple files
    """
    # Validate single file constraint for Phase 3
    files = manifest["files"]
    if len(files) != 1:
        raise RuntimeError(f"Phase 3 tabular ingestion requires exactly 1 file, got {len(files)}")

    file_entry = files[0]
    s3_key = file_entry["path"].replace("s3://landing-zone/", "")

    # Create temp file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    local_path = temp_file.name
    temp_file.close()

    try:
        _download_tabular_from_landing(
            minio=context.resources.minio,
            s3_key=s3_key,
            local_path=local_path,
            log=context.log,
        )
        return local_path
    except Exception:
        # Clean up temp file on failure
        try:
            Path(local_path).unlink(missing_ok=True)
        except Exception:
            pass
        raise


@op(
    ins={"local_path": In(dagster_type=str), "manifest": In(dagster_type=dict)},
    out={"tabular_data": Out(dagster_type=dict)},
    required_resource_keys={},
)
def load_and_clean_tabular(context: OpExecutionContext, local_path: str, manifest: dict) -> dict:
    """
    Load CSV file and apply header cleaning.

    Args:
        context: Dagster op execution context
        local_path: Path to local CSV file
        manifest: Manifest dict

    Returns:
        Dict with table, header_mapping, row_count, columns
    """
    return _load_and_clean_tabular(
        local_path=local_path,
        manifest=manifest,
        log=context.log,
    )


@op(
    ins={"tabular_data": In(dagster_type=dict), "manifest": In(dagster_type=dict)},
    out={"asset_info": Out(dagster_type=dict)},
    required_resource_keys={"minio", "mongodb"},
)
def export_tabular_parquet_to_datalake(context: OpExecutionContext, tabular_data: dict, manifest: dict) -> dict:
    """
    Export cleaned tabular data to Parquet in data lake and register asset.

    Args:
        context: Dagster op execution context
        tabular_data: Dict from load_and_clean_tabular op
        manifest: Manifest dict

    Returns:
        Asset info dict
    """
    return _export_tabular_parquet_to_datalake(
        table=tabular_data["table"],
        header_mapping=tabular_data["header_mapping"],
        minio=context.resources.minio,
        mongodb=context.resources.mongodb,
        manifest=manifest,
        run_id=context.run_id,
        log=context.log,
    )
