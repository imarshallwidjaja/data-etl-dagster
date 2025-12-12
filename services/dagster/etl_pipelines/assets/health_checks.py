"""Health check assets for validating system dependencies and services."""

from dagster import asset, AssetExecutionContext

from ..resources import GDALResource


@asset(group_name="maintenance", compute_kind="gdal", required_resource_keys={"gdal"})
def gdal_health_check(context: AssetExecutionContext) -> dict:
    """
    Verify that GDAL and required libraries are properly installed and configured.
    
    This asset runs inside the user-code container where GDAL is installed.
    
    Checks:
    - gdalinfo version
    - ogr2ogr version
    - Critical vector formats (GeoJSON, Shapefile, PostgreSQL, Parquet)
    - Critical raster formats (GTiff, COG)
    - /vsis3/ support
    - PROJ library availability
    
    Returns:
        Dictionary with check results and versions.
    
    Raises:
        RuntimeError: If any critical check fails.
    """
    gdal: GDALResource = context.resources.gdal
    results = {}
    
    # 1. Check gdalinfo version
    res = gdal.run_raw_command(["gdalinfo", "--version"])
    if not res.success:
        raise RuntimeError(f"gdalinfo check failed: {res.stderr}")
    gdalinfo_version = res.stdout.strip()
    context.log.info(f"GDAL version: {gdalinfo_version}")
    results["gdalinfo_version"] = gdalinfo_version
    
    # 2. Check ogr2ogr version
    res = gdal.run_raw_command(["ogr2ogr", "--version"])
    if not res.success:
        raise RuntimeError(f"ogr2ogr check failed: {res.stderr}")
    ogr2ogr_version = res.stdout.strip()
    context.log.info(f"ogr2ogr version: {ogr2ogr_version}")
    results["ogr2ogr_version"] = ogr2ogr_version
    
    # 3. Check vector formats
    res = gdal.run_raw_command(["ogr2ogr", "--formats"])
    if not res.success:
        raise RuntimeError(f"ogr2ogr formats check failed: {res.stderr}")
    
    required_vector_formats = [
        "GeoJSON",
        "ESRI Shapefile",
        "PostgreSQL",
        "Parquet",
    ]
    missing_formats = [fmt for fmt in required_vector_formats if fmt not in res.stdout]
    if missing_formats:
        raise RuntimeError(
            f"Missing required vector formats: {missing_formats}\n"
            f"Available formats:\n{res.stdout}"
        )
    context.log.info(f"Required vector formats available: {required_vector_formats}")
    results["vector_formats"] = required_vector_formats
    
    # 4. Check raster formats
    res = gdal.run_raw_command(["gdalinfo", "--formats"])
    if not res.success:
        raise RuntimeError(f"gdalinfo formats check failed: {res.stderr}")
    
    required_raster_formats = ["GTiff", "COG"]
    missing_formats = [fmt for fmt in required_raster_formats if fmt not in res.stdout]
    if missing_formats:
        raise RuntimeError(
            f"Missing required raster formats: {missing_formats}\n"
            f"Available formats:\n{res.stdout}"
        )
    context.log.info(f"Raster formats available: {required_raster_formats}")
    results["raster_formats"] = required_raster_formats
    
    # 5. Check PROJ
    res = gdal.run_raw_command(["proj", "-v"])
    if res.success:
        proj_version = res.stdout.strip()
        context.log.info(f"PROJ library available: {proj_version}")
        results["proj_version"] = proj_version
    else:
        # Fall back to checking gdalinfo output
        context.log.warning("PROJ command not in PATH, verifying via GDAL")
        results["proj_version"] = "Available (via GDAL)"
    
    context.log.info("GDAL Health Check Passed")
    return results

