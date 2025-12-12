"""Dagster Resources - External Service Connections."""

from .minio_resource import MinIOResource
from .postgis_resource import PostGISResource

__all__ = [
    "MinIOResource",
    "PostGISResource",
]

