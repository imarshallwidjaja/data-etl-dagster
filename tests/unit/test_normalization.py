"""Unit tests for Arrow type normalization."""

import pyarrow as pa

from libs.normalization import normalize_arrow_dtype


class TestNormalizeArrowDtype:
    """Test normalize_arrow_dtype function."""

    def test_string_types(self):
        """Test string type normalization."""
        field = pa.field("name", pa.string())
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "STRING"
        assert result["logical_type"] == "string"
        assert result["nullable"] is True

    def test_integer_types(self):
        """Test integer type normalization."""
        for dtype in [
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.uint8(),
            pa.uint16(),
            pa.uint32(),
            pa.uint64(),
        ]:
            field = pa.field("num", dtype, nullable=False)
            result = normalize_arrow_dtype(field)
            assert result["type_name"] == "INTEGER"
            assert result["nullable"] is False

    def test_float_types(self):
        """Test floating point type normalization."""
        for dtype in [pa.float16(), pa.float32(), pa.float64()]:
            field = pa.field("val", dtype)
            result = normalize_arrow_dtype(field)
            assert result["type_name"] == "FLOAT"

    def test_boolean_type(self):
        """Test boolean type normalization."""
        field = pa.field("flag", pa.bool_())
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "BOOLEAN"

    def test_timestamp_timezone_normalization(self):
        """Test that timezone is stripped for consistency."""
        field = pa.field("ts", pa.timestamp("ns", tz="UTC"))
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "TIMESTAMP"
        assert result["logical_type"] == "timestamp[ns]"  # TZ stripped

    def test_date_type(self):
        """Test date type normalization."""
        field = pa.field("dt", pa.date32())
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "DATE"

    def test_binary_type(self):
        """Test binary type normalization."""
        field = pa.field("data", pa.binary())
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "BINARY"

    def test_list_type(self):
        """Test list/array type normalization."""
        field = pa.field("items", pa.list_(pa.int32()))
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "ARRAY"

    def test_struct_type(self):
        """Test struct type normalization."""
        field = pa.field("nested", pa.struct([("x", pa.int32()), ("y", pa.int32())]))
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "STRUCT"

    def test_geometry_column_by_extension_metadata(self):
        """Test geometry detection by GeoArrow extension metadata."""
        # With geoarrow extension metadata -> GEOMETRY
        field = pa.field(
            "geom", pa.binary(), metadata={b"ARROW:extension:name": b"geoarrow.wkb"}
        )
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "GEOMETRY"

    def test_binary_column_not_assumed_geometry(self):
        """Test that binary columns are NOT assumed to be geometry by name."""
        # Without extension metadata, a column named 'geometry' is still BINARY
        field = pa.field("geometry", pa.binary())
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "BINARY"  # NOT GEOMETRY

    def test_unknown_type(self):
        """Test fallback for unknown types."""
        field = pa.field("null_col", pa.null())
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "UNKNOWN"

    def test_duration_type_is_unknown(self):
        """Test that duration type falls back to UNKNOWN."""
        field = pa.field("elapsed", pa.duration("ns"))
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "UNKNOWN"

    def test_decimal_type(self):
        """Test decimal type normalization."""
        field = pa.field("price", pa.decimal128(10, 2))
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "DECIMAL"

    def test_time_type(self):
        """Test time type normalization."""
        field = pa.field("time_col", pa.time64("ns"))
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "TIME"

    def test_map_type(self):
        """Test map type normalization."""
        field = pa.field("mapping", pa.map_(pa.string(), pa.int32()))
        result = normalize_arrow_dtype(field)
        assert result["type_name"] == "MAP"


class TestNormalizeArrowSchema:
    """Test normalize_arrow_schema function with full schemas."""

    def test_empty_schema(self):
        """Test normalization of empty schema."""
        from libs.normalization import normalize_arrow_schema

        result = normalize_arrow_schema(pa.schema([]))
        assert result == {}

    def test_basic_schema(self):
        """Test normalization of basic schema without geo metadata."""
        from libs.normalization import normalize_arrow_schema

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("active", pa.bool_()),
            ]
        )
        result = normalize_arrow_schema(schema)
        assert len(result) == 3
        assert result["id"]["type_name"] == "INTEGER"
        assert result["name"]["type_name"] == "STRING"
        assert result["active"]["type_name"] == "BOOLEAN"

    def test_geoparquet_metadata_marks_geometry(self):
        """Test that GeoParquet 'geo' metadata correctly marks geometry columns."""
        from libs.normalization import normalize_arrow_schema
        import json

        geo_metadata = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("geometry", pa.binary()),  # No field-level indicator
            ],
            metadata={b"geo": json.dumps(geo_metadata).encode()},
        )
        result = normalize_arrow_schema(schema)
        assert result["geometry"]["type_name"] == "GEOMETRY"
        assert result["id"]["type_name"] == "INTEGER"

    def test_geoparquet_with_malformed_json(self):
        """Test that malformed GeoParquet metadata doesn't crash."""
        from libs.normalization import normalize_arrow_schema

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("geom", pa.binary()),
            ],
            metadata={b"geo": b"not valid json {"},
        )
        # Should not raise, geometry column should be typed as BINARY
        result = normalize_arrow_schema(schema)
        assert result["geom"]["type_name"] == "BINARY"
        assert result["id"]["type_name"] == "INTEGER"


class TestExtractColumnSchema:
    """Test extract_column_schema helper function."""

    def test_returns_column_info_models(self):
        """Test that extract_column_schema returns ColumnInfo Pydantic models."""
        from libs.normalization import extract_column_schema
        from libs.models import ColumnInfo

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        )
        result = extract_column_schema(schema)
        assert len(result) == 2
        assert isinstance(result["id"], ColumnInfo)
        assert isinstance(result["name"], ColumnInfo)
        assert result["id"].type_name == "INTEGER"
        assert result["id"].title == "id"
        assert result["name"].type_name == "STRING"

    def test_preserves_nullability(self):
        """Test that nullability is correctly propagated."""
        from libs.normalization import extract_column_schema

        schema = pa.schema(
            [
                pa.field("required", pa.int64(), nullable=False),
                pa.field("optional", pa.int64(), nullable=True),
            ]
        )
        result = extract_column_schema(schema)
        assert result["required"].nullable is False
        assert result["optional"].nullable is True
