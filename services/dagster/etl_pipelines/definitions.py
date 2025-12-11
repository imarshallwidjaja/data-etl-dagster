"""Dagster Definitions - Repository Configuration."""

from dagster import Definitions
from libs.models import MinIOSettings

from .resources import MinIOResource

# Import assets, jobs, resources, schedules, and sensors as they are created
# Example:
# from .ops import my_asset
# from .jobs import my_job
# from .sensors import my_sensor

# Initialize resources with environment configuration
minio_settings = MinIOSettings()

defs = Definitions(
    assets=[],
    jobs=[],
    resources={
        "minio": MinIOResource(
            endpoint=minio_settings.endpoint,
            access_key=minio_settings.access_key,
            secret_key=minio_settings.secret_key,
            use_ssl=minio_settings.use_ssl,
            landing_bucket=minio_settings.landing_bucket,
            lake_bucket=minio_settings.lake_bucket,
        ),
    },
    schedules=[],
    sensors=[],
)

