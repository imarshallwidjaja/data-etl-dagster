"""Dagster Definitions - Repository Configuration."""

from dagster import Definitions, EnvVar, define_asset_job

from .resources import MinIOResource, MongoDBResource, PostGISResource, GDALResource
from .partitions import dataset_partitions  # noqa: F401
from .assets import (
    gdal_health_check,
    raw_manifest_json,
    raw_spatial_asset,
    raw_tabular_asset,
    joined_spatial_asset,
)
from .jobs import ingest_job, ingest_tabular_job
from .sensors import manifest_sensor, spatial_sensor, tabular_sensor, join_sensor

gdal_health_check_job = define_asset_job(
    "gdal_health_check_job",
    selection=[gdal_health_check],
    description="Health check for GDAL installation and dependencies",
)

join_asset_job = define_asset_job(
    "join_asset_job",
    selection=[raw_manifest_json, joined_spatial_asset],
    description="Job to materialize joined_spatial_asset (join_datasets intent)",
)

spatial_asset_job = define_asset_job(
    "spatial_asset_job",
    selection=[raw_manifest_json, raw_spatial_asset],
    description="Job to materialize raw_spatial_asset (asset-based spatial ingestion)",
)

tabular_asset_job = define_asset_job(
    "tabular_asset_job",
    selection=[raw_manifest_json, raw_tabular_asset],
    description="Job to materialize raw_tabular_asset (asset-based tabular ingestion)",
)

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
        ingest_job,
        ingest_tabular_job,
        join_asset_job,
        spatial_asset_job,
        tabular_asset_job,
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
    sensors=[manifest_sensor, spatial_sensor, tabular_sensor, join_sensor],
)

