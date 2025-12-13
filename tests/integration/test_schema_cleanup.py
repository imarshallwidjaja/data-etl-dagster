"""
Integration tests for PostGIS schema cleanup.

Tests that ephemeral schemas are properly cleaned up after processing,
ensuring PostGIS remains transient compute only (architectural law).
"""

import pytest
import psycopg2
from psycopg2 import sql

from libs.models import PostGISSettings
from libs.spatial_utils import RunIdSchemaMapping
from services.dagster.etl_pipelines.resources.postgis_resource import PostGISResource
from services.dagster.etl_pipelines.ops.cleanup_op import _cleanup_schema


pytestmark = pytest.mark.integration


@pytest.fixture
def postgis_settings():
    """Load PostGIS settings from environment."""
    return PostGISSettings()


@pytest.fixture
def postgis_resource(postgis_settings):
    """Create PostGISResource instance."""
    return PostGISResource(
        host=postgis_settings.host,
        port=postgis_settings.port,
        user=postgis_settings.user,
        password=postgis_settings.password,
        database=postgis_settings.database,
    )


@pytest.fixture
def postgis_connection(postgis_settings):
    """Create PostGIS connection."""
    conn = psycopg2.connect(
        postgis_settings.connection_string,
        connect_timeout=5,
    )
    yield conn
    conn.close()


class TestSchemaCleanup:
    """Test that schemas are properly cleaned up."""
    
    def test_cleanup_op_drops_schema(self, postgis_resource, postgis_connection):
        """Test that cleanup op successfully drops a schema."""
        # Create a test schema using PostGISResource
        test_run_id = "abc12345-def6-7890-abcd-ef1234567890"
        mapping = RunIdSchemaMapping.from_run_id(test_run_id)
        test_schema = mapping.schema_name
        
        # Create schema manually
        with postgis_connection.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(test_schema)
            ))
            postgis_connection.commit()
            
            # Verify schema exists
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s
            """, (test_schema,))
            result = cur.fetchone()
            assert result is not None, f"Schema {test_schema} should exist before cleanup"
        
        # Create schema info dict
        schema_info = {
            "schema": test_schema,
            "run_id": test_run_id,
            "manifest": {"batch_id": "test_batch"},
            "tables": ["test_table"],
        }
        
        # Create mock logger
        import logging
        mock_log = logging.getLogger("test")
        
        # Call cleanup function
        _cleanup_schema(
            postgis=postgis_resource,
            schema_info=schema_info,
            log=mock_log,
        )
        
        # Verify schema is dropped
        with postgis_connection.cursor() as cur:
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s
            """, (test_schema,))
            result = cur.fetchone()
            assert result is None, f"Schema {test_schema} should be dropped after cleanup"
    
    def test_cleanup_op_handles_missing_schema(self, postgis_resource, postgis_connection):
        """Test that cleanup op handles missing schema gracefully."""
        # Create schema info for a non-existent schema
        test_run_id = "def67890-1234-5678-9abc-def123456789"
        mapping = RunIdSchemaMapping.from_run_id(test_run_id)
        test_schema = mapping.schema_name
        
        # Verify schema doesn't exist
        with postgis_connection.cursor() as cur:
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s
            """, (test_schema,))
            result = cur.fetchone()
            assert result is None, f"Schema {test_schema} should not exist"
        
        # Create schema info dict
        schema_info = {
            "schema": test_schema,
            "run_id": test_run_id,
            "manifest": {"batch_id": "test_batch"},
            "tables": [],
        }
        
        # Create mock logger
        import logging
        mock_log = logging.getLogger("test")
        
        # Call cleanup function - should not raise
        _cleanup_schema(
            postgis=postgis_resource,
            schema_info=schema_info,
            log=mock_log,
        )
        
        # Verify schema still doesn't exist (idempotent)
        with postgis_connection.cursor() as cur:
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s
            """, (test_schema,))
            result = cur.fetchone()
            assert result is None, f"Schema {test_schema} should still not exist"
    
    def test_ephemeral_schema_lifecycle(self, postgis_resource, postgis_connection):
        """Test full ephemeral schema lifecycle: create -> use -> cleanup."""
        test_run_id = "fed98765-4321-8765-cba9-fed987654321"
        mapping = RunIdSchemaMapping.from_run_id(test_run_id)
        test_schema = mapping.schema_name
        
        # Step 1: Create schema (simulating load_op)
        with postgis_connection.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(test_schema)
            ))
            postgis_connection.commit()
            
            # Verify schema exists
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s
            """, (test_schema,))
            result = cur.fetchone()
            assert result is not None, "Schema should exist after creation"
        
        # Step 2: Use schema (simulating transform_op)
        # Create a test table in the schema
        with postgis_connection.cursor() as cur:
            cur.execute(sql.SQL("""
                CREATE TABLE {}.test_table (
                    id SERIAL PRIMARY KEY,
                    geom GEOMETRY(POINT, 4326)
                )
            """).format(sql.Identifier(test_schema)))
            postgis_connection.commit()
            
            # Verify table exists
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = 'test_table'
            """, (test_schema,))
            result = cur.fetchone()
            assert result is not None, "Table should exist in schema"
        
        # Step 3: Cleanup schema (simulating cleanup_op)
        schema_info = {
            "schema": test_schema,
            "run_id": test_run_id,
            "manifest": {"batch_id": "test_batch"},
            "tables": ["test_table"],
        }
        
        import logging
        mock_log = logging.getLogger("test")
        
        _cleanup_schema(
            postgis=postgis_resource,
            schema_info=schema_info,
            log=mock_log,
        )
        
        # Step 4: Verify schema and all contents are dropped
        with postgis_connection.cursor() as cur:
            # Verify schema is dropped
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s
            """, (test_schema,))
            result = cur.fetchone()
            assert result is None, "Schema should be dropped after cleanup"
            
            # Verify table is also gone (CASCADE should have dropped it)
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s
            """, (test_schema,))
            result = cur.fetchone()
            assert result is None, "Table should be dropped with schema (CASCADE)"

