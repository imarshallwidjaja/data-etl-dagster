"""Dagster Definitions - Repository Configuration."""

from dagster import Definitions, EnvVar, define_asset_job, AssetSelection

from .resources import MinIOResource, MongoDBResource, PostGISResource, GDALResource
from .assets import (
    gdal_health_check,
    raw_manifest_json,
    raw_spatial_asset,
    raw_tabular_asset,
    joined_spatial_asset,
)
from .jobs import ingest_job, ingest_tabular_job
from .sensors import manifest_sensor

gdal_health_check_job = define_asset_job(
    "gdal_health_check_job",
    selection=[gdal_health_check],
    description="Health check for GDAL installation and dependencies",
)

join_asset_job = define_asset_job(
    "join_asset_job",
    selection=AssetSelection.assets(joined_spatial_asset),
    description="Materialize joined spatial asset from tabular + spatial parents",
)

defs = Definitions(
    assets=[
        gdal_health_check,
        raw_manifest_json,
        raw_spatial_asset,
        raw_tabular_asset,
        joined_spatial_asset,
    ],
    jobs=[gdal_health_check_job, ingest_job, ingest_tabular_job, join_asset_job],
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
    sensors=[manifest_sensor],
)

