"""Unit tests for Arrow type normalization."""

import pytest
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
