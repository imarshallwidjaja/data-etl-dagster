#!/usr/bin/env python3
"""
Service health check script for integration tests.

Polls service endpoints until they become ready or timeout is reached.
Used by CI/CD pipelines and local development to ensure services are up before running tests.
"""

import os
import sys
import time
from typing import Callable

try:
    import requests
    import psycopg2
    from minio import Minio
    from pymongo import MongoClient
except ImportError as e:
    print(f"ERROR: Missing required dependency: {e}")
    print("Install test dependencies with: pip install -r requirements-test.txt")
    sys.exit(1)

try:
    from models import MinIOSettings, MongoSettings, PostGISSettings
except ModuleNotFoundError:
    print(
        "ERROR: Missing spatial-etl-libs package. Install with `pip install -r requirements-test.txt` "
        "or `pip install -e ./libs` before running this script."
    )
    sys.exit(1)


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


def verify_user_code_dagster(port: int = 3000, timeout: int = 30) -> bool:
    """
    Verify that user-code container is loadable by checking Dagster GraphQL API.
    
    Queries for available jobs and verifies that gdal_health_check_job exists,
    which proves that the user-code container can load modules successfully.
    
    Args:
        port: Dagster webserver port (default 3000)
        timeout: Request timeout in seconds (default 30)
        
    Returns:
        True if user-code is verified, False otherwise
    """
    try:
        url = f"http://localhost:{port}/graphql"
        
        # Query for available jobs
        query = {
            "query": """
                {
                    workspaceOrError {
                        ... on Workspace {
                            locationEntries {
                                name
                                locationOrLoadError {
                                    ... on RepositoryLocation {
                                        repositories {
                                            name
                                            jobs {
                                                name
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            """
        }
        
        response = requests.post(
            url,
            json=query,
            timeout=timeout,
        )
        
        if response.status_code != 200:
            print(f"  User-code verification: GraphQL returned status {response.status_code}")
            return False
        
        data = response.json()
        
        # Check for GraphQL errors
        if "errors" in data:
            error_messages = [err.get("message", "Unknown error") for err in data["errors"]]
            print(f"  User-code verification: GraphQL errors: {', '.join(error_messages)}")
            return False
        
        # Navigate through the response structure to find jobs
        workspace = data.get("data", {}).get("workspaceOrError", {})
        if "locationEntries" not in workspace:
            print("  User-code verification: No location entries found")
            return False
        
        # Collect all job names from all locations
        all_jobs = []
        for location_entry in workspace["locationEntries"]:
            location = location_entry.get("locationOrLoadError", {})
            if "repositories" in location:
                for repo in location["repositories"]:
                    jobs = repo.get("jobs", [])
                    all_jobs.extend([job.get("name") for job in jobs])
        
        # Check if gdal_health_check_job exists
        if "gdal_health_check_job" in all_jobs:
            print(f"  User-code verification: gdal_health_check_job found (total jobs: {len(all_jobs)})")
            return True
        else:
            print(f"  User-code verification: gdal_health_check_job not found. Available jobs: {all_jobs}")
            return False
            
    except Exception as e:
        print(f"  User-code verification failed: {e}")
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
            # NOTE: ASCII-only output for Windows console compatibility.
            # Some environments (e.g., cp1252) cannot encode Unicode glyphs like ✓/✗.
            print(f"[OK] {name} is ready (took {elapsed:.1f}s)")
            return True
        time.sleep(interval)
    
    elapsed = time.time() - start_time
    print(f"[FAIL] {name} failed to become ready after {elapsed:.1f}s")
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
        ("User-code (Dagster)", lambda: verify_user_code_dagster(dagster_port, timeout)),
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

