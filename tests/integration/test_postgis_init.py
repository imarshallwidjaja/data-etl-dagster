"""
Integration tests for PostGIS initialization verification.

Verifies that PostGIS initialization scripts have run correctly by checking:
- Required extensions exist (postgis, postgis_topology, postgis_raster, fuzzystrmatch, uuid-ossp)
- Expected utility functions exist (create_processing_schema, drop_processing_schema, etc.)
"""

import pytest
import psycopg2

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


class TestPostGISInitialization:
    """Test PostGIS initialization invariants."""
    
    def test_required_extensions_exist(self, postgis_connection):
        """Test that all required PostGIS extensions are installed."""
        with postgis_connection.cursor() as cur:
            cur.execute("""
                SELECT extname, extversion 
                FROM pg_extension 
                WHERE extname IN ('postgis', 'postgis_topology', 'postgis_raster', 'fuzzystrmatch', 'uuid-ossp')
                ORDER BY extname;
            """)
            installed_extensions = {row[0]: row[1] for row in cur.fetchall()}
        
        required_extensions = [
            "postgis",
            "postgis_topology",
            "postgis_raster",
            "fuzzystrmatch",
            "uuid-ossp",
        ]
        
        for ext_name in required_extensions:
            assert ext_name in installed_extensions, \
                f"Required extension '{ext_name}' not found. " \
                f"Installed extensions: {list(installed_extensions.keys())}"
    
    def test_create_processing_schema_function_exists(self, postgis_connection):
        """Test that create_processing_schema function exists."""
        with postgis_connection.cursor() as cur:
            cur.execute("""
                SELECT proname, proargtypes::regtype[]
                FROM pg_proc
                WHERE proname = 'create_processing_schema'
                AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');
            """)
            result = cur.fetchone()
            
            assert result is not None, \
                "Function 'create_processing_schema' not found"
            assert result[0] == "create_processing_schema"
    
    def test_drop_processing_schema_function_exists(self, postgis_connection):
        """Test that drop_processing_schema function exists."""
        with postgis_connection.cursor() as cur:
            cur.execute("""
                SELECT proname, proargtypes::regtype[]
                FROM pg_proc
                WHERE proname = 'drop_processing_schema'
                AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');
            """)
            result = cur.fetchone()
            
            assert result is not None, \
                "Function 'drop_processing_schema' not found"
            assert result[0] == "drop_processing_schema"
    
    def test_list_processing_schemas_function_exists(self, postgis_connection):
        """Test that list_processing_schemas function exists."""
        with postgis_connection.cursor() as cur:
            cur.execute("""
                SELECT proname
                FROM pg_proc
                WHERE proname = 'list_processing_schemas'
                AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');
            """)
            result = cur.fetchone()
            
            assert result is not None, \
                "Function 'list_processing_schemas' not found"
            assert result[0] == "list_processing_schemas"
    
    def test_cleanup_all_processing_schemas_function_exists(self, postgis_connection):
        """Test that cleanup_all_processing_schemas function exists."""
        with postgis_connection.cursor() as cur:
            cur.execute("""
                SELECT proname
                FROM pg_proc
                WHERE proname = 'cleanup_all_processing_schemas'
                AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');
            """)
            result = cur.fetchone()
            
            assert result is not None, \
                "Function 'cleanup_all_processing_schemas' not found"
            assert result[0] == "cleanup_all_processing_schemas"
    
    def test_processing_schema_functions_work(self, postgis_connection):
        """Test that processing schema utility functions can be called."""
        test_run_id = "test_init_verification_12345"
        
        with postgis_connection.cursor() as cur:
            # Test create_processing_schema
            cur.execute("SELECT create_processing_schema(%s);", (test_run_id,))
            schema_name = cur.fetchone()[0]
            assert schema_name.startswith("proc_")
            
            # Verify schema was created
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s;
            """, (schema_name,))
            assert cur.fetchone() is not None, f"Schema {schema_name} was not created"
            
            # Test drop_processing_schema
            cur.execute("SELECT drop_processing_schema(%s);", (test_run_id,))
            result = cur.fetchone()[0]
            assert result is True
            
            # Verify schema was dropped
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s;
            """, (schema_name,))
            assert cur.fetchone() is None, f"Schema {schema_name} was not dropped"
            
            postgis_connection.commit()

