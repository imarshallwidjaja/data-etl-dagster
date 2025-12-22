"""Arrow type normalization utilities for columnar schema capture."""

import json
import logging
from typing import TYPE_CHECKING, TypedDict

import pyarrow as pa
import pyarrow.types as pat

if TYPE_CHECKING:
    from libs.models import ColumnInfo

__all__ = [
    "normalize_arrow_dtype",
    "normalize_arrow_schema",
    "extract_column_schema",
    "NormalizedType",
]

logger = logging.getLogger(__name__)


class NormalizedType(TypedDict):
    """Normalized type information for a column."""

    type_name: str  # Canonical category: STRING, INTEGER, FLOAT, etc.
    logical_type: str  # Detailed type: int64, float32, timestamp[ns]
    nullable: bool


# Canonical type vocabulary
TYPE_STRING = "STRING"
TYPE_INTEGER = "INTEGER"
TYPE_FLOAT = "FLOAT"
TYPE_BOOLEAN = "BOOLEAN"
TYPE_TIMESTAMP = "TIMESTAMP"
TYPE_DATE = "DATE"
TYPE_TIME = "TIME"
TYPE_DECIMAL = "DECIMAL"
TYPE_BINARY = "BINARY"
TYPE_ARRAY = "ARRAY"
TYPE_STRUCT = "STRUCT"
TYPE_MAP = "MAP"
TYPE_GEOMETRY = "GEOMETRY"
TYPE_UNKNOWN = "UNKNOWN"


def normalize_arrow_schema(schema: pa.Schema) -> dict[str, "NormalizedType"]:
    """
    Normalize an entire PyArrow schema, handling GeoParquet global metadata.
    """
    column_schema = {}

    # Check for GeoParquet metadata in schema
    geometry_columns = set()
    if schema.metadata and b"geo" in schema.metadata:
        try:
            geo_md = json.loads(schema.metadata[b"geo"].decode())
            if "columns" in geo_md:
                geometry_columns.update(geo_md["columns"].keys())
                logger.debug(
                    f"Detected geometry columns from GeoParquet metadata: {list(geometry_columns)}"
                )
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse GeoParquet 'geo' metadata as JSON: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error reading GeoParquet metadata: {e}")

    for field in schema:
        is_geom_hint = field.name in geometry_columns
        normalized = normalize_arrow_dtype(field, is_geometry_hint=is_geom_hint)
        column_schema[field.name] = normalized

    return column_schema


def extract_column_schema(schema: pa.Schema) -> dict[str, "ColumnInfo"]:
    """
    Extract column schema from PyArrow schema for asset metadata.

    This is a convenience function that combines normalize_arrow_schema()
    with ColumnInfo model instantiation, eliminating duplicated code
    across ops files (export_op, join_ops, tabular_ops).

    Args:
        schema: PyArrow schema from table or Parquet file

    Returns:
        Dict mapping column name to ColumnInfo Pydantic model

    Example:
        >>> import pyarrow as pa
        >>> schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
        >>> column_schema = extract_column_schema(schema)
        >>> column_schema["id"].type_name
        'INTEGER'
    """
    # Import here to avoid circular dependency
    from libs.models import ColumnInfo

    normalized = normalize_arrow_schema(schema)
    return {
        name: ColumnInfo(
            title=name,
            description="",
            type_name=info["type_name"],
            logical_type=info["logical_type"],
            nullable=info["nullable"],
        )
        for name, info in normalized.items()
    }


def normalize_arrow_dtype(
    field: pa.Field, is_geometry_hint: bool = False
) -> NormalizedType:
    """
    Normalize PyArrow field to canonical type representation.

    Uses PyArrow's type checking API for version stability instead of
    string parsing. Returns both canonical type category and detailed
    logical type for full introspection capability.

    Args:
        field: PyArrow Field with name and type information

    Returns:
        NormalizedType dict with type_name, logical_type, nullable

    Example:
        >>> field = pa.field("age", pa.int64(), nullable=False)
        >>> normalize_arrow_dtype(field)
        {'type_name': 'INTEGER', 'logical_type': 'int64', 'nullable': False}
    """
    dtype = field.type
    nullable = field.nullable

    # Determine canonical type category using PyArrow type checking API
    if pat.is_string(dtype) or pat.is_large_string(dtype) or pat.is_unicode(dtype):
        type_name = TYPE_STRING
    elif pat.is_integer(dtype):
        type_name = TYPE_INTEGER
    elif pat.is_floating(dtype):
        type_name = TYPE_FLOAT
    elif pat.is_boolean(dtype):
        type_name = TYPE_BOOLEAN
    elif pat.is_timestamp(dtype):
        type_name = TYPE_TIMESTAMP
    elif pat.is_date(dtype):
        type_name = TYPE_DATE
    elif pat.is_time(dtype):
        type_name = TYPE_TIME
    elif pat.is_decimal(dtype):
        type_name = TYPE_DECIMAL
    elif (
        pat.is_binary(dtype)
        or pat.is_large_binary(dtype)
        or pat.is_fixed_size_binary(dtype)
    ):
        if is_geometry_hint or _is_geometry_type(field):
            type_name = TYPE_GEOMETRY
        else:
            type_name = TYPE_BINARY
    elif (
        pat.is_list(dtype) or pat.is_large_list(dtype) or pat.is_fixed_size_list(dtype)
    ):
        type_name = TYPE_ARRAY
    elif pat.is_struct(dtype):
        # Check for geometry columns (GeoArrow WKB extension type or struct with wkb)
        if is_geometry_hint or _is_geometry_type(field):
            type_name = TYPE_GEOMETRY
        else:
            type_name = TYPE_STRUCT
    elif pat.is_map(dtype):
        type_name = TYPE_MAP
    else:
        type_name = TYPE_UNKNOWN

    # Get detailed logical type, stripping timezone for consistency
    logical_type = _get_logical_type_string(dtype)

    return NormalizedType(
        type_name=type_name,
        logical_type=logical_type,
        nullable=nullable,
    )


def _is_geometry_type(field: pa.Field) -> bool:
    """
    Check if a field represents geometry data.

    Detects geometry via GeoArrow extension type OR field metadata.
    """
    dtype = field.type

    # 1. Check for ExtensionType (the "proper" Arrow way)
    if isinstance(dtype, pa.ExtensionType):
        ext_name = dtype.extension_name
        if ext_name.startswith("geoarrow.") or "wkb" in ext_name.lower():
            return True

    # 2. Check for GeoArrow extension type metadata keys in field metadata
    # (some writers store extension-ish info here without defining a full ExtensionType)
    if field.metadata:
        # Standard Arrow extension name key
        ext_name = field.metadata.get(b"ARROW:extension:name")
        if ext_name:
            ext_name_str = ext_name.decode()
            if ext_name_str.startswith("geoarrow.") or "wkb" in ext_name_str.lower():
                return True

        # Check for presence of GeoParquet metadata in field (sometimes stored as json)
        # Note: Usually GeoParquet stores column info in file schema metadata,
        # but individual fields might have 'geometry' markers.
        if b"geometry" in field.metadata or b"geo" in field.metadata:
            return True

    return False


def _get_logical_type_string(dtype: pa.DataType) -> str:
    """
    Get a stable logical type string, normalizing timezone representations.
    """
    type_str = str(dtype)

    # Normalize timestamp timezone representations for consistency
    # timestamp[ns, tz=UTC] -> timestamp[ns]
    # timestamp[us, tz=America/New_York] -> timestamp[us]
    if "timestamp" in type_str and ", tz=" in type_str:
        # Keep base timestamp type without timezone for catalog consistency
        type_str = type_str.split(", tz=")[0] + "]"

    return type_str
