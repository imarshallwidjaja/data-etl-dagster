#!/usr/bin/env python3
"""
Service health check script for integration tests.

Polls service endpoints until they become ready or timeout is reached.
Used by CI/CD pipelines and local development to ensure services are up before running tests.
"""

import os
import sys
import time
from typing import Callable, Optional

try:
    import requests
    import psycopg2
    from minio import Minio
    from pymongo import MongoClient
    from tenacity import retry, stop_after_attempt, wait_exponential
except ImportError as e:
    print(f"ERROR: Missing required dependency: {e}")
    print("Install test dependencies with: pip install -r requirements-test.txt")
    sys.exit(1)

# Add parent directory to path to import libs
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from libs.models import MinIOSettings, MongoSettings, PostGISSettings


# =============================================================================
# Health Check Functions
# =============================================================================

def check_minio(settings: MinIOSettings, timeout: int = 30) -> bool:
    """Check if MinIO is ready and accessible."""
    try:
        client = Minio(
            settings.endpoint,
            access_key=settings.access_key,
            secret_key=settings.secret_key,
            secure=settings.use_ssl,
        )
        # Try to list buckets (requires connection)
        client.list_buckets()
        return True
    except Exception as e:
        print(f"  MinIO not ready: {e}")
        return False


def check_mongodb(settings: MongoSettings, timeout: int = 30) -> bool:
    """Check if MongoDB is ready and accessible."""
    try:
        client = MongoClient(
            settings.connection_string,
            serverSelectionTimeoutMS=timeout * 1000,
        )
        # Ping the server
        client.admin.command("ping")
        client.close()
        return True
    except Exception as e:
        print(f"  MongoDB not ready: {e}")
        return False


def check_postgis(settings: PostGISSettings, timeout: int = 30) -> bool:
    """Check if PostGIS is ready and accessible."""
    try:
        conn = psycopg2.connect(
            settings.connection_string,
            connect_timeout=timeout,
        )
        with conn.cursor() as cur:
            # Check PostgreSQL version
            cur.execute("SELECT version();")
            pg_version = cur.fetchone()[0]
            
            # Check PostGIS extension
            cur.execute("SELECT PostGIS_Version();")
            postgis_version = cur.fetchone()[0]
            
            if not postgis_version:
                print("  PostGIS extension not found")
                return False
                
        conn.close()
        return True
    except Exception as e:
        print(f"  PostGIS not ready: {e}")
        return False


def check_dagster(port: int = 3000, timeout: int = 30) -> bool:
    """Check if Dagster GraphQL API is ready."""
    try:
        url = f"http://localhost:{port}/graphql"
        response = requests.post(
            url,
            json={"query": "{ version }"},
            timeout=timeout,
        )
        if response.status_code == 200:
            data = response.json()
            if data.get("data", {}).get("version"):
                return True
        print(f"  Dagster GraphQL returned status {response.status_code}")
        return False
    except Exception as e:
        print(f"  Dagster not ready: {e}")
        return False


# =============================================================================
# Retry Logic
# =============================================================================

def wait_for_service(
    name: str,
    check_fn: Callable[[], bool],
    timeout: int = 60,
    interval: int = 2,
) -> bool:
    """
    Wait for a service to become ready.
    
    Args:
        name: Service name for logging
        check_fn: Function that returns True when service is ready
        timeout: Maximum time to wait in seconds
        interval: Time between checks in seconds
    
    Returns:
        True if service became ready, False if timeout reached
    """
    print(f"Waiting for {name}...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        if check_fn():
            elapsed = time.time() - start_time
            print(f"✓ {name} is ready (took {elapsed:.1f}s)")
            return True
        time.sleep(interval)
    
    elapsed = time.time() - start_time
    print(f"✗ {name} failed to become ready after {elapsed:.1f}s")
    return False


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Wait for all services to become ready."""
    print("=" * 60)
    print("Service Health Check")
    print("=" * 60)
    
    # Load settings from environment
    try:
        minio_settings = MinIOSettings()
        mongo_settings = MongoSettings()
        postgis_settings = PostGISSettings()
    except Exception as e:
        print(f"ERROR: Failed to load settings: {e}")
        print("Make sure .env file exists or environment variables are set")
        sys.exit(1)
    
    # Get Dagster port from environment
    dagster_port = int(os.getenv("DAGSTER_WEBSERVER_PORT", "3000"))
    
    # Timeout per service (in seconds)
    timeout = int(os.getenv("SERVICE_WAIT_TIMEOUT", "60"))
    
    # Check all services
    services = [
        ("MinIO", lambda: check_minio(minio_settings, timeout)),
        ("MongoDB", lambda: check_mongodb(mongo_settings, timeout)),
        ("PostGIS", lambda: check_postgis(postgis_settings, timeout)),
        ("Dagster", lambda: check_dagster(dagster_port, timeout)),
    ]
    
    failed = []
    for name, check_fn in services:
        if not wait_for_service(name, check_fn, timeout=timeout):
            failed.append(name)
    
    print("=" * 60)
    if failed:
        print(f"FAILED: {', '.join(failed)} did not become ready")
        sys.exit(1)
    else:
        print("SUCCESS: All services are ready")
        sys.exit(0)


if __name__ == "__main__":
    main()

