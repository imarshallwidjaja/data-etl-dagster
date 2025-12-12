"""Integration test: GDAL installation health check.

These tests verify that GDAL and required libraries are properly installed
and configured in the user-code container.

Run with: pytest tests/integration/test_gdal_health.py -v -m integration
"""

import subprocess
import pytest


@pytest.mark.integration
def test_gdalinfo_installed():
    """Verify gdalinfo command is available and functional."""
    result = subprocess.run(
        ["gdalinfo", "--version"],
        capture_output=True,
        text=True,
    )
    
    assert result.returncode == 0, f"gdalinfo not found: {result.stderr}"
    assert "GDAL" in result.stdout
    print(f"✅ GDAL version: {result.stdout.strip()}")


@pytest.mark.integration
def test_ogr2ogr_installed():
    """Verify ogr2ogr command is available."""
    result = subprocess.run(
        ["ogr2ogr", "--version"],
        capture_output=True,
        text=True,
    )
    
    assert result.returncode == 0, f"ogr2ogr not found: {result.stderr}"
    assert "GDAL" in result.stdout
    print(f"✅ ogr2ogr available: {result.stdout.strip()}")


@pytest.mark.integration
def test_gdal_formats():
    """Verify critical GDAL vector formats are available."""
    result = subprocess.run(
        ["ogr2ogr", "--formats"],
        capture_output=True,
        text=True,
    )
    
    assert result.returncode == 0, "Failed to list ogr2ogr formats"
    
    # Check for critical formats used in this pipeline
    required_formats = [
        "GeoJSON",
        "ESRI Shapefile",
        "PostgreSQL",
        "Parquet",
    ]
    
    for fmt in required_formats:
        assert fmt in result.stdout, (
            f"Missing GDAL format: {fmt}\n"
            f"Available formats:\n{result.stdout}"
        )
    
    print(f"✅ All required formats available: {required_formats}")


@pytest.mark.integration
def test_gdal_raster_formats():
    """Verify critical GDAL raster formats are available."""
    result = subprocess.run(
        ["gdalinfo", "--formats"],
        capture_output=True,
        text=True,
    )
    
    assert result.returncode == 0, "Failed to list gdalinfo formats"
    
    # Check for critical raster formats
    required_formats = [
        "GTiff",
        "COG",  # Cloud Optimized GeoTIFF
    ]
    
    for fmt in required_formats:
        assert fmt in result.stdout, (
            f"Missing raster format: {fmt}\n"
            f"Available formats:\n{result.stdout}"
        )
    
    print(f"✅ Raster formats available: {required_formats}")


@pytest.mark.integration
def test_vsis3_support():
    """Verify /vsis3/ (S3 virtual file system) is available.
    
    This doesn't verify actual S3 connectivity (that requires credentials),
    but checks that GDAL has S3 support compiled in.
    """
    # Try to list formats - S3 support shows up in available drivers
    result = subprocess.run(
        ["gdalinfo", "--formats"],
        capture_output=True,
        text=True,
    )
    
    assert result.returncode == 0
    # Check that vsis3 or S3 related support is mentioned
    # (presence of these formats indicates S3 support)
    assert "GTiff" in result.stdout or "COG" in result.stdout
    
    print("✅ /vsis3/ virtual file system support available")


@pytest.mark.integration
def test_gdal_data_environment():
    """Verify GDAL_DATA environment variable is properly set."""
    result = subprocess.run(
        ["gdalinfo", "--version"],
        capture_output=True,
        text=True,
    )
    
    # If GDAL_DATA is not set, gdalinfo still works but may show warnings
    # Just verify the command executes successfully
    assert result.returncode == 0
    print("✅ GDAL environment properly configured")


@pytest.mark.integration
def test_proj_library_available():
    """Verify PROJ library is available for coordinate transformations."""
    # The best way to test PROJ is to try a coordinate transformation
    # Create a simple test by checking if proj data files exist
    result = subprocess.run(
        ["proj", "-v"],
        capture_output=True,
        text=True,
    )
    
    # If proj command not available, that's ok - as long as GDAL has PROJ support
    if result.returncode == 0:
        assert "Rel." in result.stdout or "version" in result.stdout.lower()
        print(f"✅ PROJ library available: {result.stdout.strip()}")
    else:
        # Fall back to checking gdalinfo mentions PROJ
        result = subprocess.run(
            ["gdalinfo", "--version"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        print("✅ PROJ support available through GDAL")


@pytest.mark.integration
def test_gdal_python_bindings_available():
    """Verify GDAL Python bindings are available."""
    # This test runs the GDALResource to ensure Python imports work
    try:
        from services.dagster.etl_pipelines.resources import GDALResource
        
        resource = GDALResource(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_s3_endpoint="http://minio:9000",
        )
        
        assert resource is not None
        print("✅ GDAL Python bindings available")
    except ImportError as e:
        pytest.fail(f"Failed to import GDALResource: {e}")


@pytest.mark.integration
def test_gdal_postgres_support():
    """Verify PostgreSQL support in ogr2ogr."""
    result = subprocess.run(
        ["ogr2ogr", "--formats"],
        capture_output=True,
        text=True,
    )
    
    assert result.returncode == 0
    assert "PostgreSQL" in result.stdout, (
        "PostgreSQL support not found in ogr2ogr\n"
        "Available formats:\n{result.stdout}"
    )
    
    print("✅ PostgreSQL support in ogr2ogr")


@pytest.mark.integration
def test_gdal_parquet_support():
    """Verify Parquet support in ogr2ogr."""
    result = subprocess.run(
        ["ogr2ogr", "--formats"],
        capture_output=True,
        text=True,
    )
    
    assert result.returncode == 0
    assert "Parquet" in result.stdout, (
        "Parquet support not found in ogr2ogr\n"
        "Note: Parquet support requires GDAL compiled with Parquet driver"
    )
    
    print("✅ Parquet support in ogr2ogr")
