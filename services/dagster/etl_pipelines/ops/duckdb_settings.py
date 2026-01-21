"""
DuckDB configuration defaults for join_asset.

Centralizes version pin guidance and runtime settings for S3 access and
out-of-core execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from ..resources.minio_resource import MinIOResource

DUCKDB_VERSION_PIN = "1.4.0"
DEFAULT_DUCKDB_MEMORY_LIMIT = "1GB"
DEFAULT_DUCKDB_TEMP_SUBDIR = "duckdb"


@dataclass(frozen=True)
class DuckDBJoinSettings:
    s3_endpoint: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_url_style: str
    s3_use_ssl: bool
    temp_directory: str
    memory_limit: str


def build_duckdb_join_settings(
    *,
    minio: MinIOResource,
    temp_dir: Path,
    memory_limit: str = DEFAULT_DUCKDB_MEMORY_LIMIT,
) -> DuckDBJoinSettings:
    endpoint = minio.endpoint
    if "://" in endpoint:
        parsed = urlparse(endpoint)
        endpoint = parsed.netloc or parsed.path

    return DuckDBJoinSettings(
        s3_endpoint=endpoint,
        s3_access_key_id=minio.access_key,
        s3_secret_access_key=minio.secret_key,
        s3_url_style="path",
        s3_use_ssl=minio.use_ssl,
        temp_directory=str(temp_dir),
        memory_limit=memory_limit,
    )
