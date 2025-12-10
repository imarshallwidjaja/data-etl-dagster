# =============================================================================
# Configuration Models Module
# =============================================================================
# Provides Pydantic Settings models for all service configurations:
# - MinIOSettings: S3-compatible object storage configuration
# - MongoSettings: MongoDB metadata ledger configuration
# - PostGISSettings: PostGIS spatial compute engine configuration
# - DagsterPostgresSettings: Dagster internal metadata database configuration
# =============================================================================

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

__all__ = [
    "MinIOSettings",
    "MongoSettings",
    "PostGISSettings",
    "DagsterPostgresSettings",
]


# =============================================================================
# MinIO Settings (S3-Compatible Object Storage)
# =============================================================================

class MinIOSettings(BaseSettings):
    """
    Configuration for MinIO (S3-compatible object storage).
    
    Maps environment variables with prefix "MINIO_":
    - MINIO_ENDPOINT → endpoint
    - MINIO_ROOT_USER → access_key
    - MINIO_ROOT_PASSWORD → secret_key
    - MINIO_USE_SSL → use_ssl
    - MINIO_LANDING_BUCKET → landing_bucket
    - MINIO_LAKE_BUCKET → lake_bucket
    
    Attributes:
        endpoint: MinIO server endpoint (host:port)
        access_key: Access key (maps from MINIO_ROOT_USER)
        secret_key: Secret key (maps from MINIO_ROOT_PASSWORD)
        use_ssl: Whether to use SSL/TLS (default: False)
        landing_bucket: Landing zone bucket name (default: "landing-zone")
        lake_bucket: Data lake bucket name (default: "data-lake")
    """
    
    endpoint: str = Field(..., validation_alias="MINIO_ENDPOINT", description="MinIO server endpoint (host:port)")
    access_key: str = Field(..., validation_alias="MINIO_ROOT_USER", description="Access key (maps from MINIO_ROOT_USER)")
    secret_key: str = Field(..., validation_alias="MINIO_ROOT_PASSWORD", description="Secret key (maps from MINIO_ROOT_PASSWORD)")
    use_ssl: bool = Field(False, validation_alias="MINIO_USE_SSL", description="Whether to use SSL/TLS")
    landing_bucket: str = Field("landing-zone", validation_alias="MINIO_LANDING_BUCKET", description="Landing zone bucket name")
    lake_bucket: str = Field("data-lake", validation_alias="MINIO_LAKE_BUCKET", description="Data lake bucket name")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",  # Ignore unrelated env vars from shared .env files
    )


# =============================================================================
# MongoDB Settings (Metadata Ledger)
# =============================================================================

class MongoSettings(BaseSettings):
    """
    Configuration for MongoDB (metadata ledger).
    
    Maps environment variables with prefix "MONGO_":
    - MONGO_HOST → host
    - MONGO_PORT → port
    - MONGO_INITDB_ROOT_USERNAME → username
    - MONGO_INITDB_ROOT_PASSWORD → password
    - MONGO_DATABASE → database
    - MONGO_AUTH_SOURCE → auth_source
    
    Attributes:
        host: MongoDB host (default: "mongodb")
        port: MongoDB port (default: 27017)
        username: MongoDB username (maps from MONGO_INITDB_ROOT_USERNAME)
        password: MongoDB password (maps from MONGO_INITDB_ROOT_PASSWORD)
        database: Database name (default: "spatial_etl")
        auth_source: Authentication source (default: "admin")
    """
    
    host: str = Field("mongodb", validation_alias="MONGO_HOST", description="MongoDB host")
    port: int = Field(27017, validation_alias="MONGO_PORT", description="MongoDB port")
    username: str = Field(..., validation_alias="MONGO_INITDB_ROOT_USERNAME", description="MongoDB username (maps from MONGO_INITDB_ROOT_USERNAME)")
    password: str = Field(..., validation_alias="MONGO_INITDB_ROOT_PASSWORD", description="MongoDB password (maps from MONGO_INITDB_ROOT_PASSWORD)")
    database: str = Field("spatial_etl", validation_alias="MONGO_DATABASE", description="Database name")
    auth_source: str = Field("admin", validation_alias="MONGO_AUTH_SOURCE", description="Authentication source")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",  # Ignore unrelated env vars from shared .env files
    )
    
    @property
    def connection_string(self) -> str:
        """
        Build MongoDB connection URI.
        
        Format: mongodb://[username]:[password]@[host]:[port]/[database]?authSource=[auth_source]
        
        Returns:
            MongoDB connection URI string
        """
        return (
            f"mongodb://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}?authSource={self.auth_source}"
        )


# =============================================================================
# PostGIS Settings (Spatial Compute Engine)
# =============================================================================

class PostGISSettings(BaseSettings):
    """
    Configuration for PostGIS (spatial compute engine).
    
    PostGIS is used as a transient compute engine (Load → Transform → Dump → Drop).
    Ephemeral schemas are created per run and dropped after processing.
    
    Maps environment variables with prefix "POSTGRES_":
    - POSTGRES_HOST → host
    - POSTGRES_PORT → port
    - POSTGRES_USER → user
    - POSTGRES_PASSWORD → password
    - POSTGRES_DB → database
    
    Attributes:
        host: PostGIS host (default: "postgis")
        port: PostGIS port (default: 5432)
        user: PostgreSQL user
        password: PostgreSQL password
        database: Database name (default: "spatial_compute")
    """
    
    host: str = Field("postgis", validation_alias="POSTGRES_HOST", description="PostGIS host")
    port: int = Field(5432, validation_alias="POSTGRES_PORT", description="PostGIS port")
    user: str = Field(..., validation_alias="POSTGRES_USER", description="PostgreSQL user")
    password: str = Field(..., validation_alias="POSTGRES_PASSWORD", description="PostgreSQL password")
    database: str = Field("spatial_compute", validation_alias="POSTGRES_DB", description="Database name")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",  # Ignore unrelated env vars from shared .env files
    )
    
    @property
    def connection_string(self) -> str:
        """
        Build PostgreSQL connection URI.
        
        Format: postgresql://[user]:[password]@[host]:[port]/[database]
        
        Returns:
            PostgreSQL connection URI string
        """
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )


# =============================================================================
# Dagster Postgres Settings (Internal Metadata DB)
# =============================================================================

class DagsterPostgresSettings(BaseSettings):
    """
    Configuration for Dagster's internal PostgreSQL metadata database.
    
    This is separate from PostGIS. Dagster uses this database to store:
    - Run history
    - Asset materializations
    - Schedule/ sensor state
    - Other workflow metadata
    
    Maps environment variables with prefix "DAGSTER_POSTGRES_":
    - DAGSTER_POSTGRES_HOST → host
    - DAGSTER_POSTGRES_PORT → port
    - DAGSTER_POSTGRES_USER → user
    - DAGSTER_POSTGRES_PASSWORD → password
    - DAGSTER_POSTGRES_DB → database
    
    Attributes:
        host: PostgreSQL host
        port: PostgreSQL port (default: 5433)
        user: PostgreSQL user
        password: PostgreSQL password
        database: Database name
    """
    
    host: str = Field(..., validation_alias="DAGSTER_POSTGRES_HOST", description="PostgreSQL host")
    port: int = Field(5433, validation_alias="DAGSTER_POSTGRES_PORT", description="PostgreSQL port")
    user: str = Field(..., validation_alias="DAGSTER_POSTGRES_USER", description="PostgreSQL user")
    password: str = Field(..., validation_alias="DAGSTER_POSTGRES_PASSWORD", description="PostgreSQL password")
    database: str = Field(..., validation_alias="DAGSTER_POSTGRES_DB", description="Database name")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",  # Ignore unrelated env vars from shared .env files
    )
    
    @property
    def connection_string(self) -> str:
        """
        Build PostgreSQL connection URI for Dagster.
        
        Format: postgresql://[user]:[password]@[host]:[port]/[database]
        
        Returns:
            PostgreSQL connection URI string
        """
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )

