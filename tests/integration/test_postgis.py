"""
Integration tests for PostGIS (spatial compute engine).

Tests basic connectivity, PostgreSQL version, and PostGIS extension availability.
"""

import pytest
import psycopg2
from psycopg2 import sql

from libs.models import PostGISSettings


pytestmark = pytest.mark.integration


@pytest.fixture
def postgis_settings():
    """Load PostGIS settings from environment."""
    return PostGISSettings()


@pytest.fixture
def postgis_connection(postgis_settings):
    """Create PostGIS connection."""
    conn = psycopg2.connect(
        postgis_settings.connection_string,
        connect_timeout=5,
    )
    yield conn
    conn.close()


class TestPostGISConnectivity:
    """Test PostGIS connectivity and basic operations."""
    
    def test_postgresql_connection(self, postgis_connection):
        """Test that PostgreSQL is accessible."""
        with postgis_connection.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            assert "PostgreSQL" in version
    
    def test_postgis_extension_available(self, postgis_connection):
        """Test that PostGIS extension is installed and available."""
        with postgis_connection.cursor() as cur:
            # Verify extension is installed
            cur.execute("SELECT extname, extversion FROM pg_extension WHERE extname = 'postgis';")
            ext_row = cur.fetchone()
            assert ext_row is not None, "postgis extension is not installed"

            # Check PostGIS version
            cur.execute("SELECT PostGIS_Version();")
            postgis_version = cur.fetchone()[0]
            assert postgis_version is not None, "PostGIS_Version() returned empty"
            # Accept common PostGIS version string patterns (format differs by build)
            assert any(
                token in postgis_version.upper()
                for token in ["POSTGIS", "USE_GEOS", "USE_PROJ"]
            ), f"Unexpected PostGIS version string: {postgis_version}"
    
    def test_postgis_spatial_functions(self, postgis_connection):
        """Test that PostGIS spatial functions work."""
        with postgis_connection.cursor() as cur:
            # Test ST_MakePoint (basic spatial function)
            cur.execute("SELECT ST_MakePoint(0, 0);")
            point = cur.fetchone()[0]
            assert point is not None
            
            # Test ST_AsText (convert geometry to text)
            cur.execute("SELECT ST_AsText(ST_MakePoint(0, 0));")
            text = cur.fetchone()[0]
            assert text == "POINT(0 0)"
    
    def test_ephemeral_schema_creation(self, postgis_connection):
        """Test that we can create and drop ephemeral schemas (compute engine pattern)."""
        test_schema = "test_integration_schema"
        
        with postgis_connection.cursor() as cur:
            # Create schema
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
            assert result is not None
            assert result[0] == test_schema
            
            # Drop schema (cleanup)
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(test_schema)
            ))
            postgis_connection.commit()
            
            # Verify schema is dropped
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s
            """, (test_schema,))
            result = cur.fetchone()
            assert result is None

