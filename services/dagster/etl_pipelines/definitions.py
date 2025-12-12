"""Dagster Definitions - Repository Configuration."""

from dagster import Definitions, define_asset_job
from libs.models import MinIOSettings, MongoSettings, PostGISSettings, GDALSettings

from .resources import MinIOResource, MongoDBResource, PostGISResource, GDALResource
from .assets import gdal_health_check

# Import assets, jobs, resources, schedules, and sensors as they are created
# Example:
# from .ops import my_asset
# from .jobs import my_job
# from .sensors import my_sensor

# Initialize resources with environment configuration
minio_settings = MinIOSettings()
mongo_settings = MongoSettings()
postgis_settings = PostGISSettings()
gdal_settings = GDALSettings()

gdal_health_check_job = define_asset_job(
    "gdal_health_check_job",
    selection=[gdal_health_check],
    description="Health check for GDAL installation and dependencies",
)

defs = Definitions(
    assets=[gdal_health_check],
    jobs=[gdal_health_check_job],
    resources={
        "minio": MinIOResource(
            endpoint=minio_settings.endpoint,
            access_key=minio_settings.access_key,
            secret_key=minio_settings.secret_key,
            use_ssl=minio_settings.use_ssl,
            landing_bucket=minio_settings.landing_bucket,
            lake_bucket=minio_settings.lake_bucket,
        ),
        "mongodb": MongoDBResource(
            connection_string=mongo_settings.connection_string,
            database=mongo_settings.database,
        ),
        "postgis": PostGISResource(
            host=postgis_settings.host,
            port=postgis_settings.port,
            user=postgis_settings.user,
            password=postgis_settings.password,
            database=postgis_settings.database,
        ),
        "gdal": GDALResource(
            aws_access_key_id=gdal_settings.aws_access_key_id,
            aws_secret_access_key=gdal_settings.aws_secret_access_key,
            aws_s3_endpoint=gdal_settings.aws_s3_endpoint,
            gdal_data_path=gdal_settings.gdal_data_path,
            proj_lib_path=gdal_settings.proj_lib_path,
        ),
    },
    schedules=[],
    sensors=[],
)

