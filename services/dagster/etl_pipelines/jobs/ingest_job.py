"""Legacy spatial ingestion job (op-based).

This job uses ops directly without asset tracking. It is kept for
backwards compatibility. New spatial ingestion should use spatial_asset_job
which properly materializes raw_spatial_asset with partition support.
"""

from dagster import job

from ..ops import load_to_postgis, spatial_transform, export_to_datalake


@job(
    name="ingest_job",
    description="Main ingestion job: Loads spatial data from landing zone to PostGIS, transforms it, and exports to data lake",
)
def ingest_job():
    """
    Main ingestion job triggered by manifest sensor.
    
    Pipeline flow:
    1. load_to_postgis: Loads spatial data from MinIO landing zone to PostGIS ephemeral schema
    2. spatial_transform: Executes spatial transformations using recipe-based architecture
    3. export_to_datalake: Exports processed data to MinIO data lake and registers in MongoDB
    
    The manifest is passed as an op input to load_to_postgis via run config.
    """
    schema_info = load_to_postgis()
    transform_result = spatial_transform(schema_info)
    export_to_datalake(transform_result)

