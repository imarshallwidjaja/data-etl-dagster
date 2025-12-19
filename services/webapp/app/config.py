# =============================================================================
# Webapp Configuration
# =============================================================================
# Settings loaded from environment variables.
# =============================================================================

from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # MinIO Configuration
    minio_endpoint: str = "minio:9000"
    minio_root_user: str = "minio"
    minio_root_password: str = "minio_password"
    minio_use_ssl: bool = False
    minio_landing_bucket: str = "landing-zone"
    minio_lake_bucket: str = "data-lake"

    # MongoDB Configuration
    mongo_connection_string: str = (
        "mongodb://mongo:mongo_password@mongodb:27017/spatial_etl?authSource=admin"
    )

    # Dagster Configuration
    dagster_graphql_url: str = "http://dagster-webserver:3000/graphql"

    # Webapp Authentication
    webapp_username: str = "admin"
    webapp_password: str = "admin"

    # SQLite Database (ephemeral session data)
    database_url: str = "sqlite:///./webapp.db"

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
