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


def verify_mongodb_initialization(settings: MongoSettings) -> bool:
    """
    Verify that MongoDB initialization scripts ran successfully.
    
    Checks that required collections exist: assets, manifests, runs, lineage.
    
    Args:
        settings: MongoDB connection settings
        
    Returns:
        True if all required collections exist, False otherwise
    """
    try:
        client = MongoClient(
            settings.connection_string,
            serverSelectionTimeoutMS=10000,
        )
        db = client[settings.database]
        
        required_collections = ["assets", "manifests", "runs", "lineage"]
        existing_collections = db.list_collection_names()
        
        missing = [col for col in required_collections if col not in existing_collections]
        
        if missing:
            print(f"  MongoDB initialization incomplete: missing collections: {', '.join(missing)}")
            client.close()
            return False
        
        print(f"  MongoDB initialization verified: all collections exist")
        client.close()
        return True
    except Exception as e:
        print(f"  MongoDB initialization verification failed: {e}")
        return False


def verify_postgis_initialization(settings: PostGISSettings) -> bool:
    """
    Verify that PostGIS initialization scripts ran successfully.
    
    Checks that required extensions are loaded:
    - postgis
    - postgis_topology
    - postgis_raster
    - fuzzystrmatch
    - uuid-ossp
    
    Args:
        settings: PostGIS connection settings
        
    Returns:
        True if all required extensions exist, False otherwise
    """
    try:
        conn = psycopg2.connect(
            settings.connection_string,
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            # Check for required extensions
            required_extensions = [
                "postgis",
                "postgis_topology",
                "postgis_raster",
                "fuzzystrmatch",
                "uuid-ossp",
            ]
            
            cur.execute("""
                SELECT extname 
                FROM pg_extension 
                WHERE extname = ANY(%s)
            """, (required_extensions,))
            
            found_extensions = [row[0] for row in cur.fetchall()]
            missing = [ext for ext in required_extensions if ext not in found_extensions]
            
            if missing:
                print(f"  PostGIS initialization incomplete: missing extensions: {', '.join(missing)}")
                conn.close()
                return False
            
            print(f"  PostGIS initialization verified: all extensions loaded")
            conn.close()
            return True
    except Exception as e:
        print(f"  PostGIS initialization verification failed: {e}")
        return False


def verify_user_code_dagster(port: int = 3000, timeout: int = 30) -> bool:
    """
    Verify that user-code container can load Dagster modules and jobs are available.
    
    Queries Dagster GraphQL API to check that gdal_health_check_job exists,
    which proves the user-code container can successfully load modules.
    
    Args:
        port: Dagster webserver port (default 3000)
        timeout: Request timeout in seconds (default 30)
        
    Returns:
        True if gdal_health_check_job is available, False otherwise
    """
    try:
        url = f"http://localhost:{port}/graphql"
        
        # Query for available jobs
        query = {
            "query": """
                {
                    jobsOrError {
                        ... on Jobs {
                            results {
                                name
                            }
                        }
                        ... on PythonError {
                            message
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
            print(f"  Dagster GraphQL returned status {response.status_code}")
            return False
        
        data = response.json()
        
        # Check for GraphQL errors
        if "errors" in data:
            print(f"  Dagster GraphQL errors: {data['errors']}")
            return False
        
        # Check for Python errors in response
        jobs_data = data.get("data", {}).get("jobsOrError", {})
        if "message" in jobs_data:
            print(f"  Dagster Python error: {jobs_data['message']}")
            return False
        
        # Extract job names
        jobs = jobs_data.get("results", [])
        job_names = [job.get("name") for job in jobs]
        
        # Verify gdal_health_check_job exists
        if "gdal_health_check_job" not in job_names:
            print(f"  User-code verification failed: gdal_health_check_job not found")
            print(f"  Available jobs: {', '.join(job_names) if job_names else 'none'}")
            return False
        
        print(f"  User-code verification successful: gdal_health_check_job available")
        print(f"  Available jobs: {', '.join(job_names)}")
        return True
        
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
    
    # Verify initialization after services are ready
    if "MongoDB" not in failed:
        print("\nVerifying MongoDB initialization...")
        if not verify_mongodb_initialization(mongo_settings):
            failed.append("MongoDB initialization")
    
    if "PostGIS" not in failed:
        print("\nVerifying PostGIS initialization...")
        if not verify_postgis_initialization(postgis_settings):
            failed.append("PostGIS initialization")
    
    if "Dagster" not in failed:
        print("\nVerifying user-code Dagster integration...")
        if not verify_user_code_dagster(dagster_port, timeout):
            failed.append("User-code Dagster verification")
    
    print("=" * 60)
    if failed:
        print(f"FAILED: {', '.join(failed)}")
        sys.exit(1)
    else:
        print("SUCCESS: All services are ready and initialized")
        sys.exit(0)


if __name__ == "__main__":
    main()

