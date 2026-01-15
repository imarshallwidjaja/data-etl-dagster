"""Minimal integration test fixtures - wiring only, no large helper bodies.

Large helper bodies live in tests/integration/helpers.py.
This file provides pytest fixtures that wire up those helpers.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generator

import pytest
from minio import Minio
from pymongo import MongoClient

from .helpers import DagsterGraphQLClient, wait_for_graphql_ready

if TYPE_CHECKING:
    from psycopg2.extensions import connection as Psycopg2Connection


@dataclass
class MinIOSettingsData:
    endpoint: str
    access_key: str
    secret_key: str
    use_ssl: bool
    landing_bucket: str
    lake_bucket: str


@dataclass
class MongoSettingsData:
    database: str


@pytest.fixture
def dagster_graphql_url() -> str:
    port = os.getenv("DAGSTER_WEBSERVER_PORT", "3000")
    return f"http://localhost:{port}/graphql"


@pytest.fixture
def dagster_client(dagster_graphql_url: str) -> DagsterGraphQLClient:
    return wait_for_graphql_ready(dagster_graphql_url, timeout=30)


@pytest.fixture
def minio_endpoint() -> str:
    return os.getenv("MINIO_ENDPOINT", "localhost:9000")


@pytest.fixture
def minio_access_key() -> str:
    return os.getenv("MINIO_ACCESS_KEY", "minioadmin")


@pytest.fixture
def minio_secret_key() -> str:
    return os.getenv("MINIO_SECRET_KEY", "minioadmin")


@pytest.fixture
def minio_use_ssl() -> bool:
    return os.getenv("MINIO_USE_SSL", "false").lower() == "true"


@pytest.fixture
def minio_landing_bucket() -> str:
    return os.getenv("MINIO_LANDING_BUCKET", "landing-zone")


@pytest.fixture
def minio_lake_bucket() -> str:
    return os.getenv("MINIO_LAKE_BUCKET", "data-lake")


@pytest.fixture
def minio_settings(
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    minio_use_ssl: bool,
    minio_landing_bucket: str,
    minio_lake_bucket: str,
) -> MinIOSettingsData:
    return MinIOSettingsData(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        use_ssl=minio_use_ssl,
        landing_bucket=minio_landing_bucket,
        lake_bucket=minio_lake_bucket,
    )


@pytest.fixture
def minio_client(
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    minio_use_ssl: bool,
    minio_landing_bucket: str,
    minio_lake_bucket: str,
) -> Minio:
    client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_use_ssl,
    )

    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            client.list_buckets()
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("MinIO did not become ready within timeout")

    for bucket in [minio_landing_bucket, minio_lake_bucket]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

    return client


@pytest.fixture
def mongo_host() -> str:
    return os.getenv("MONGO_HOST", "localhost")


@pytest.fixture
def mongo_port() -> int:
    return int(os.getenv("MONGO_PORT", "27017"))


@pytest.fixture
def mongo_username() -> str:
    return os.getenv("MONGO_USERNAME", "admin")


@pytest.fixture
def mongo_password() -> str:
    return os.getenv("MONGO_PASSWORD", "admin")


@pytest.fixture
def mongo_database() -> str:
    return os.getenv("MONGO_DATABASE", "etl_metadata")


@pytest.fixture
def mongo_settings(mongo_database: str) -> MongoSettingsData:
    return MongoSettingsData(database=mongo_database)


@pytest.fixture
def mongo_auth_source() -> str:
    return os.getenv("MONGO_AUTH_SOURCE", "admin")


@pytest.fixture
def mongo_connection_string(
    mongo_host: str,
    mongo_port: int,
    mongo_username: str,
    mongo_password: str,
    mongo_auth_source: str,
) -> str:
    return (
        f"mongodb://{mongo_username}:{mongo_password}@{mongo_host}:{mongo_port}"
        f"/?authSource={mongo_auth_source}"
    )


@pytest.fixture
def mongo_client(mongo_connection_string: str) -> MongoClient[dict[str, object]]:
    return MongoClient(
        mongo_connection_string,
        serverSelectionTimeoutMS=5000,
    )


@pytest.fixture
def postgis_host() -> str:
    return os.getenv("POSTGIS_HOST", "localhost")


@pytest.fixture
def postgis_port() -> int:
    return int(os.getenv("POSTGIS_PORT", "5432"))


@pytest.fixture
def postgis_user() -> str:
    return os.getenv("POSTGIS_USER", "postgres")


@pytest.fixture
def postgis_password() -> str:
    return os.getenv("POSTGIS_PASSWORD", "postgres")


@pytest.fixture
def postgis_database() -> str:
    return os.getenv("POSTGIS_DATABASE", "etl_db")


@pytest.fixture
def postgis_connection_string(
    postgis_host: str,
    postgis_port: int,
    postgis_user: str,
    postgis_password: str,
    postgis_database: str,
) -> str:
    return (
        f"postgresql://{postgis_user}:{postgis_password}@"
        f"{postgis_host}:{postgis_port}/{postgis_database}"
    )


@pytest.fixture
def postgis_connection(
    postgis_connection_string: str,
) -> Generator["Psycopg2Connection", None, None]:
    import psycopg2

    conn = psycopg2.connect(
        postgis_connection_string,
        connect_timeout=5,
    )
    yield conn
    conn.close()
