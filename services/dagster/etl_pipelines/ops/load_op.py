# =============================================================================
# Load Op - S3 to PostGIS
# =============================================================================
# Loads spatial data from MinIO landing zone to PostGIS ephemeral schema
# using GDAL ogr2ogr.
# =============================================================================

from typing import Dict, Any
from dagster import op, OpExecutionContext, In, Out
from sqlalchemy import text

from libs.spatial_utils import RunIdSchemaMapping
from libs.s3_utils import s3_to_vsis3
from libs.models import Manifest
from ..resources.gdal_resource import GDALResult
import json
from typing import Dict as DictType, List


def _extract_schema_from_gdal_json(json_output: str) -> DictType[str, Any]:
    """
    Extract normalized schema information from GDAL ogrinfo JSON output.

    Args:
        json_output: Raw JSON string from ogrinfo -json

    Returns:
        Dict with normalized schema info:
        - fields: dict mapping field_name -> normalized_type
        - geometry_type: string (e.g., "Point", "MultiPolygon")
        - layer_name: string

    Raises:
        ValueError: If JSON is invalid or schema is malformed
    """
    try:
        data = json.loads(json_output)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON from ogrinfo: {e}")

    if not isinstance(data, dict) or "layers" not in data:
        raise ValueError("GDAL JSON output missing 'layers' key")

    layers = data["layers"]
    if not isinstance(layers, list) or len(layers) == 0:
        raise ValueError("No layers found in dataset")

    if len(layers) > 1:
        raise ValueError("Multi-layer datasets not supported for schema comparison")

    layer = layers[0]
    layer_name = layer.get("name", "")

    # Extract geometry type
    geometry_fields = layer.get("geometryFields", [])
    if len(geometry_fields) == 0:
        raise ValueError("No geometry fields found in layer")
    if len(geometry_fields) > 1:
        raise ValueError("Multiple geometry fields not supported")

    geometry_type = geometry_fields[0].get("type", "").replace(" ", "")  # Remove spaces

    # Extract and normalize field types
    fields = {}
    for field in layer.get("fields", []):
        field_name = field.get("name", "")
        field_type = field.get("type", "")

        # Normalize common GDAL types to canonical strings
        normalized_type = field_type.replace(" ", "").lower()
        if "integer" in normalized_type:
            normalized_type = "integer"
        elif "real" in normalized_type or "float" in normalized_type:
            normalized_type = "real"
        elif "string" in normalized_type or "text" in normalized_type:
            normalized_type = "string"
        elif "date" in normalized_type:
            normalized_type = "date"
        elif "time" in normalized_type:
            normalized_type = "time"
        elif "datetime" in normalized_type:
            normalized_type = "datetime"

        fields[field_name] = normalized_type

    return {
        "fields": fields,
        "geometry_type": geometry_type,
        "layer_name": layer_name,
    }


def _load_files_to_postgis(
    gdal,
    postgis,
    manifest: Dict[str, Any],
    run_id: str,
    log,
    geom_column_name: str = "geom",
) -> Dict[str, Any]:
    """
    Core logic for loading files from S3 to PostGIS.

    This function is extracted for easier unit testing without Dagster context.

    Args:
        gdal: GDALResource instance
        postgis: PostGISResource instance
        manifest: Manifest dict with files list
        run_id: Dagster run ID
        log: Logger instance (context.log)
        geom_column_name: Name for the geometry column (default: "geom")

    Returns:
        Schema info dict with schema name, manifest, tables, run_id, and geom_column

    Raises:
        RuntimeError: If any ogr2ogr call fails or schema compatibility check fails
    """
    # Validate manifest structure and normalize to ensure all required fields exist
    if not isinstance(manifest, dict):
        raise ValueError(
            f"Expected manifest to be a dict, got {type(manifest)}: {manifest}"
        )
    
    # Unwrap 'value' key if present (Dagster wraps op inputs in 'value' when passed via run config)
    # This handles both direct manifest dicts and Dagster-wrapped manifests
    if "value" in manifest and isinstance(manifest.get("value"), dict):
        # Check if this looks like a Dagster-wrapped input (only 'value' key, or 'value' contains manifest fields)
        value_dict = manifest["value"]
        # If 'value' contains manifest-like keys (batch_id, files, etc.), use it
        if any(key in value_dict for key in ["batch_id", "files", "uploader", "intent"]):
            manifest = value_dict
    
    # Validate against Pydantic model to ensure structure is correct
    try:
        validated_manifest = Manifest(**manifest)
        # Convert back to dict for processing (ensures all fields are present)
        manifest = validated_manifest.model_dump(mode="json")
    except Exception as e:
        raise ValueError(
            f"Manifest validation failed: {e}. "
            f"Manifest keys: {list(manifest.keys()) if isinstance(manifest, dict) else 'N/A'}. "
            f"Manifest content: {manifest}"
        ) from e
    
    # Additional validation for files list
    if not isinstance(manifest["files"], list):
        raise ValueError(
            f"Expected manifest['files'] to be a list, got {type(manifest['files'])}: {manifest['files']}"
        )
    
    if len(manifest["files"]) == 0:
        raise ValueError("Manifest 'files' list is empty")
    
    # Map run_id to schema name
    mapping = RunIdSchemaMapping.from_run_id(run_id)
    schema = mapping.schema_name
    
    # Pre-flight: validate schema compatibility across all files (fail-fast)
    # This runs BEFORE schema creation to avoid creating schema if preflight fails
    if len(manifest["files"]) > 1:
        log.info("Validating schema compatibility across multiple files...")
        first_file_schema = None

        for file_entry in manifest["files"]:
            s3_path = file_entry["path"]
            vsis3_path = s3_to_vsis3(s3_path)

            # Get layer info using ogrinfo JSON output
            layer_info = gdal.ogrinfo(vsis3_path, layer=None, as_json=True)

            if not layer_info.success:
                raise RuntimeError(f"Failed to inspect schema for file {s3_path}: {layer_info.stderr}")

            # Extract normalized schema from JSON output
            try:
                current_schema = _extract_schema_from_gdal_json(layer_info.stdout)
            except ValueError as e:
                raise RuntimeError(f"Schema extraction failed for file {s3_path}: {e}")

            if first_file_schema is None:
                first_file_schema = current_schema
                log.info(f"Reference schema from {s3_path}: {len(current_schema['fields'])} fields, geometry type '{current_schema['geometry_type']}'")
            else:
                # Compare schemas (order-insensitive)
                if current_schema["fields"] != first_file_schema["fields"]:
                    field_diff = set(current_schema["fields"].keys()) ^ set(first_file_schema["fields"].keys())
                    type_mismatches = []
                    for field_name in set(current_schema["fields"].keys()) & set(first_file_schema["fields"].keys()):
                        if current_schema["fields"][field_name] != first_file_schema["fields"][field_name]:
                            type_mismatches.append(f"{field_name}: {first_file_schema['fields'][field_name]} vs {current_schema['fields'][field_name]}")

                    error_msg = f"Schema mismatch detected for file {s3_path}."
                    if field_diff:
                        error_msg += f" Field differences: {sorted(field_diff)}."
                    if type_mismatches:
                        error_msg += f" Type mismatches: {type_mismatches}."
                    raise RuntimeError(error_msg)

                if current_schema["geometry_type"] != first_file_schema["geometry_type"]:
                    raise RuntimeError(
                        f"Geometry type mismatch for file {s3_path}: "
                        f"'{first_file_schema['geometry_type']}' vs '{current_schema['geometry_type']}'"
                    )

                # Note: layer name differences are allowed (they're often auto-generated)

        log.info("Schema compatibility validation passed.")

    # Create schema explicitly before loading (ensures cleanup on failure)
    postgis._create_schema(schema)
    log.info(f"Created ephemeral schema: {schema}")

    # Build PostgreSQL connection string for ogr2ogr
    # Format: PG:host={host} dbname={database} user={user} password={password}
    pg_conn_str = (
        f"PG:host={postgis.host} "
        f"dbname={postgis.database} "
        f"user={postgis.user} "
        f"password={postgis.password}"
    )
    
    # Load all files into the same table: raw_data
    layer_name = f"{schema}.raw_data"

    try:
        for i, file_entry in enumerate(manifest["files"]):
            s3_path = file_entry["path"]

            # Convert s3:// path to /vsis3/ path for GDAL
            vsis3_path = s3_to_vsis3(s3_path)

            log.info(f"Loading file: {s3_path} -> {layer_name}")

            # CRS is inferred from source data; uploader-specified CRS is no longer accepted
            target_crs = None

            # Set options based on whether this is the first file or subsequent files
            # First file: create table with -overwrite and geometry column name
            # Subsequent files: append with -append and -update
            if i == 0:
                # First file: create table
                options = {
                    "-overwrite": "",
                    "-lco": f"GEOMETRY_NAME={geom_column_name}",
                }
            else:
                # Subsequent files: append to existing table
                options = {
                    "-append": "",
                    "-update": "",
                    "-lco": f"GEOMETRY_NAME={geom_column_name}",
                }

            # Execute ogr2ogr
            result: GDALResult = gdal.ogr2ogr(
                input_path=vsis3_path,
                output_path=pg_conn_str,
                output_format="PostgreSQL",
                layer_name=layer_name,
                target_crs=target_crs,
                options=options,
            )

            # Check for failure
            if not result.success:
                raise RuntimeError(
                    f"ogr2ogr failed for file {s3_path}: {result.stderr}"
                )

            log.info(f"Successfully loaded {s3_path} to {layer_name}")
        
        # Collect table names (single table approach)
        tables = ["raw_data"]
        
        # Return schema info (schema persists for use by subsequent ops)
        return {
            "schema": schema,
            "manifest": manifest,
            "tables": tables,
            "run_id": run_id,
            "geom_column": geom_column_name,
        }
    except Exception:
        # Clean up schema on failure to maintain architectural law
        log.warning(f"Load failed, cleaning up schema: {schema}")
        try:
            postgis._drop_schema(schema)
        except Exception as cleanup_error:
            log.warning(f"Failed to cleanup schema {schema}: {cleanup_error}")
        raise


@op(
    ins={"manifest": In(dagster_type=dict)},
    out={"schema_info": Out(dagster_type=dict)},
    required_resource_keys={"gdal", "postgis", "minio"},
)
def load_to_postgis(context: OpExecutionContext, manifest: dict) -> dict:
    """
    Load spatial data from MinIO landing zone to PostGIS ephemeral schema.

    Uses GDAL ogr2ogr to load files from S3 to PostGIS. All files in the
    manifest are loaded into a single table: raw_data. Geometry column is
    standardized to "geom" using GDAL layer creation options.

    Args:
        context: Dagster op execution context
        manifest: Manifest dict with batch_id, files, metadata, etc.

    Returns:
        Schema info dict containing:
        - schema: Schema name (e.g., "proc_abc12345_def6_7890_...")
        - manifest: Full manifest dict
        - tables: List of table names created (["raw_data"])
        - run_id: Dagster run ID
        - geom_column: Geometry column name ("geom")

    Raises:
        RuntimeError: If any ogr2ogr call fails or schema compatibility check fails
    """
    return _load_files_to_postgis(
        gdal=context.resources.gdal,
        postgis=context.resources.postgis,
        manifest=manifest,
        run_id=context.run_id,
        log=context.log,
        geom_column_name="geom",
    )

