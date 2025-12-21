"""Dagster Definitions - Repository Configuration.

Defines assets, jobs, resources, and sensors for the Spatial ETL Pipeline.
"""

from dagster import Definitions, EnvVar, define_asset_job

from .assets import (
    gdal_health_check,
    joined_spatial_asset,
    raw_manifest_json,
    raw_spatial_asset,
    raw_tabular_asset,
)
from .jobs import ingest_job  # Only legacy op-based job
from .partitions import dataset_partitions
from .resources import GDALResource, MinIOResource, MongoDBResource, PostGISResource
from .sensors import (
    join_sensor,
    manifest_sensor,
    spatial_sensor,
    tabular_sensor,
    manifest_run_failure_sensor,
    manifest_run_success_sensor,
)


# =============================================================================
# Asset Jobs
# =============================================================================

gdal_health_check_job = define_asset_job(
    "gdal_health_check_job",
    selection=[gdal_health_check],
    description="Health check for GDAL installation and dependencies",
)

spatial_asset_job = define_asset_job(
    "spatial_asset_job",
    selection=[raw_manifest_json, raw_spatial_asset],
    partitions_def=dataset_partitions,
    description="Materialize raw_spatial_asset with partition support",
)

tabular_asset_job = define_asset_job(
    "tabular_asset_job",
    selection=[raw_manifest_json, raw_tabular_asset],
    partitions_def=dataset_partitions,
    description="Materialize raw_tabular_asset with partition support",
)

join_asset_job = define_asset_job(
    "join_asset_job",
    selection=[raw_manifest_json, joined_spatial_asset],
    partitions_def=dataset_partitions,
    description="Materialize joined_spatial_asset (join_datasets intent)",
)


# =============================================================================
# Definitions
# =============================================================================

defs = Definitions(
    assets=[
        gdal_health_check,
        raw_manifest_json,
        raw_spatial_asset,
        raw_tabular_asset,
        joined_spatial_asset,
    ],
    jobs=[
        gdal_health_check_job,
        ingest_job,  # Legacy op-based job
        spatial_asset_job,  # Asset-based spatial
        tabular_asset_job,  # Asset-based tabular
        join_asset_job,  # Asset-based join
    ],
    resources={
        "minio": MinIOResource(
            endpoint=EnvVar("MINIO_ENDPOINT"),
            access_key=EnvVar("MINIO_ROOT_USER"),
            secret_key=EnvVar("MINIO_ROOT_PASSWORD"),
            use_ssl=False,
            landing_bucket="landing-zone",
            lake_bucket="data-lake",
        ),
        "mongodb": MongoDBResource(
            connection_string=EnvVar("MONGO_CONNECTION_STRING"),
            database="spatial_etl",
        ),
        "postgis": PostGISResource(
            host=EnvVar("POSTGRES_HOST"),
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            port=5432,
            database="spatial_compute",
        ),
        "gdal": GDALResource(
            aws_access_key_id=EnvVar("MINIO_ROOT_USER"),
            aws_secret_access_key=EnvVar("MINIO_ROOT_PASSWORD"),
            aws_s3_endpoint=EnvVar("MINIO_ENDPOINT"),
        ),
    },
    schedules=[],
    sensors=[
        manifest_sensor,  # Legacy: routes to ingest_job only
        spatial_sensor,  # Asset-based: routes to spatial_asset_job
        tabular_sensor,  # Asset-based: routes to tabular_asset_job
        join_sensor,  # Asset-based: routes to join_asset_job
        manifest_run_failure_sensor,  # Lifecycle: updates manifest on failure
        manifest_run_success_sensor,  # Lifecycle: updates manifest on success
    ],
)
