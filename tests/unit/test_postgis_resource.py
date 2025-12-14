"""
Unit tests for PostGISResource.

Tests ephemeral schema lifecycle and SQL operations with mocked SQLAlchemy engine.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from services.dagster.etl_pipelines.resources import PostGISResource
from libs.models import Bounds


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def postgis_resource():
    """Create a PostGISResource instance with test configuration."""
    return PostGISResource(
        host="localhost",
        port=5432,
        user="test_user",
        password="test_password",
        database="test_db",
    )


# =============================================================================
# Test: Connection String Generation
# =============================================================================

def test_get_connection_string(postgis_resource):
    """Test that connection string is formatted correctly."""
    conn_str = postgis_resource._get_connection_string()
    
    assert conn_str == "postgresql://test_user:test_password@localhost:5432/test_db"


def test_get_connection_string_with_special_chars_in_password():
    """Test connection string with special characters in password."""
    resource = PostGISResource(
        host="remote.server.com",
        port=5433,
        user="user@domain",
        password="p@ssw0rd!",
        database="mydb",
    )
    conn_str = resource._get_connection_string()
    
    assert "user@domain" in conn_str
    assert "p@ssw0rd!" in conn_str


# =============================================================================
# Test: get_engine
# =============================================================================

def test_get_engine_creates_engine_on_first_call(postgis_resource):
    """Test that get_engine creates SQLAlchemy engine."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.create_engine') as mock_create:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine
        
        # Create new instance to avoid cached engine
        resource = PostGISResource(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            database="test_db",
        )
        
        # First call creates engine
        engine1 = resource.get_engine()
        
        # Verify create_engine was called with correct parameters
        mock_create.assert_called_once()
        call_kwargs = mock_create.call_args[1]
        assert call_kwargs["pool_pre_ping"] is True
        
        # Second call returns cached engine
        engine2 = resource.get_engine()
        assert engine1 is engine2
        assert mock_create.call_count == 1


def test_get_engine_uses_connection_string():
    """Test that get_engine uses correct connection string."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.create_engine') as mock_create:
        mock_create.return_value = MagicMock()
        
        resource = PostGISResource(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            database="test_db",
        )
        resource.get_engine()
        
        conn_str_arg = mock_create.call_args[0][0]
        assert conn_str_arg == "postgresql://test_user:test_password@localhost:5432/test_db"


# =============================================================================
# Test: ephemeral_schema - Context Manager
# =============================================================================

def test_ephemeral_schema_creates_on_entry(postgis_resource):
    """Test that ephemeral_schema creates schema on entry."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        mock_get_engine.return_value = mock_engine
        
        run_id = "abc12345-def6-7890-abcd-ef1234567890"
        
        with postgis_resource.ephemeral_schema(run_id) as schema:
            # Schema name should be sanitized
            assert schema == "proc_abc12345_def6_7890_abcd_ef1234567890"
            
            # Verify execute was called (schema creation)
            assert mock_conn.execute.called
            assert mock_conn.commit.called


def test_ephemeral_schema_drops_on_exit(postgis_resource):
    """Test that ephemeral_schema drops schema on exit (normal case)."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        
        # Mock context manager for connect()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        
        mock_get_engine.return_value = mock_engine
        
        run_id = "abc12345-def6-7890-abcd-ef1234567890"
        
        # Track number of connect() calls to verify both create and drop happened
        initial_call_count = mock_engine.connect.call_count
        
        with postgis_resource.ephemeral_schema(run_id):
            pass  # Exit context manager
        
        # Verify connect() was called multiple times (create + drop)
        # (schema creation calls connect once, drop calls connect once more)
        assert mock_engine.connect.call_count >= initial_call_count + 2


def test_ephemeral_schema_drops_on_exception(postgis_resource):
    """Test that ephemeral_schema drops schema even when exception occurs."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        
        mock_get_engine.return_value = mock_engine
        
        run_id = "abc12345-def6-7890-abcd-ef1234567890"
        
        # Track connect() calls before exception
        initial_call_count = mock_engine.connect.call_count
        
        # Context manager should drop schema even though exception occurs
        with pytest.raises(ValueError):
            with postgis_resource.ephemeral_schema(run_id):
                raise ValueError("Simulated processing error")
        
        # Verify drop happened despite the exception (connect called at least twice)
        assert mock_engine.connect.call_count >= initial_call_count + 2


# =============================================================================
# Test: Schema Name Sanitization
# =============================================================================

def test_schema_name_sanitization_with_hyphens(postgis_resource):
    """Test that hyphens in run_id are replaced with underscores."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        
        mock_get_engine.return_value = mock_engine
        
        run_id = "abc12345-def6-7890-abcd-ef1234567890"
        
        with postgis_resource.ephemeral_schema(run_id) as schema:
            expected = "proc_abc12345_def6_7890_abcd_ef1234567890"
            assert schema == expected


def test_sanitize_schema_name_adds_quotes(postgis_resource):
    """Test that _sanitize_schema_name wraps name in quotes."""
    result = postgis_resource._sanitize_schema_name("proc_abc123")
    
    assert result == '"proc_abc123"'


# =============================================================================
# Test: execute_sql
# =============================================================================

def test_execute_sql_sets_search_path(postgis_resource):
    """Test that execute_sql sets search_path before execution."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        
        mock_get_engine.return_value = mock_engine
        
        schema = "proc_abc123"
        sql = "CREATE TABLE test AS SELECT 1"
        
        postgis_resource.execute_sql(sql, schema)
        
        # Verify execute was called multiple times (set search_path + user SQL)
        assert mock_conn.execute.call_count >= 2


def test_execute_sql_executes_provided_sql(postgis_resource):
    """Test that execute_sql executes the provided SQL statement."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        
        mock_get_engine.return_value = mock_engine
        
        schema = "proc_abc123"
        sql = "CREATE TABLE test AS SELECT 1"
        
        postgis_resource.execute_sql(sql, schema)
        
        # Verify execute and commit were called (indicating SQL execution)
        assert mock_conn.execute.call_count >= 2  # set search_path + user SQL
        assert mock_conn.commit.called


def test_execute_sql_commits_transaction(postgis_resource):
    """Test that execute_sql commits the transaction."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        
        mock_get_engine.return_value = mock_engine
        
        postgis_resource.execute_sql("SELECT 1", "proc_abc123")
        
        # Verify commit was called
        assert mock_conn.commit.called


# =============================================================================
# Test: table_exists
# =============================================================================

def test_table_exists_returns_true_when_table_exists(postgis_resource):
    """Test that table_exists returns True when table is found."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        
        # Mock result that indicates table exists
        mock_result = MagicMock()
        mock_result.scalar.return_value = True
        mock_conn.execute.return_value = mock_result
        
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        
        mock_get_engine.return_value = mock_engine
        
        exists = postgis_resource.table_exists("proc_abc123", "raw_input")
        
        assert exists is True


def test_table_exists_returns_false_when_table_missing(postgis_resource):
    """Test that table_exists returns False when table is not found."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        
        # Mock result that indicates table doesn't exist
        mock_result = MagicMock()
        mock_result.scalar.return_value = False
        mock_conn.execute.return_value = mock_result
        
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        
        mock_get_engine.return_value = mock_engine
        
        exists = postgis_resource.table_exists("proc_abc123", "missing_table")
        
        assert exists is False


def test_table_exists_queries_information_schema(postgis_resource):
    """Test that table_exists queries information_schema correctly."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        
        mock_result = MagicMock()
        mock_result.scalar.return_value = True
        mock_conn.execute.return_value = mock_result
        
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        
        mock_get_engine.return_value = mock_engine
        
        postgis_resource.table_exists("proc_abc123", "my_table")
        
        # Verify query included schema and table name parameters
        execute_call_args = mock_conn.execute.call_args
        params = execute_call_args[0][1]  # Second argument is params dict
        
        assert params["schema"] == "proc_abc123"
        assert params["table"] == "my_table"


# =============================================================================
# Test: get_table_bounds
# =============================================================================

def test_get_table_bounds_returns_bounds_object(postgis_resource):
    """Test that get_table_bounds returns a Bounds model instance."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.table_exists') as mock_exists:
        with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
            mock_exists.return_value = True

            mock_engine = MagicMock()
            mock_conn = MagicMock()

            # Mock column check result (geometry column exists)
            mock_column_result = MagicMock()
            mock_column_result.scalar.return_value = True
            mock_conn.execute.side_effect = [mock_column_result, MagicMock()]

            # Mock ST_Extent result (minx, miny, maxx, maxy)
            mock_extent_result = MagicMock()
            mock_extent_result.fetchone.return_value = (-180.0, -90.0, 180.0, 90.0)
            mock_conn.execute.side_effect = [mock_column_result, mock_extent_result]

            mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)

            mock_get_engine.return_value = mock_engine

            bounds = postgis_resource.get_table_bounds("proc_abc123", "features")

            assert isinstance(bounds, Bounds)
            assert bounds.minx == -180.0
            assert bounds.miny == -90.0
            assert bounds.maxx == 180.0
            assert bounds.maxy == 90.0


def test_get_table_bounds_raises_on_missing_table(postgis_resource):
    """Test that get_table_bounds raises ValueError if table doesn't exist."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.table_exists') as mock_exists:
        mock_exists.return_value = False

        with pytest.raises(ValueError, match="Table does not exist"):
            postgis_resource.get_table_bounds("proc_abc123", "missing_table", geom_column="geom")


def test_get_table_bounds_returns_none_on_no_geometry_data(postgis_resource):
    """Test that get_table_bounds returns None if table has no geometry data."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.table_exists') as mock_exists:
        with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
            mock_exists.return_value = True

            mock_engine = MagicMock()
            mock_conn = MagicMock()

            # Mock column check result (geometry column exists)
            mock_column_result = MagicMock()
            mock_column_result.scalar.return_value = True

            # Mock result where ST_Extent returned NULL (empty geometry)
            mock_extent_result = MagicMock()
            mock_extent_result.fetchone.return_value = (None, None, None, None)
            mock_conn.execute.side_effect = [mock_column_result, mock_extent_result]

            mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)

            mock_get_engine.return_value = mock_engine

            # Should return None instead of raising
            bounds = postgis_resource.get_table_bounds("proc_abc123", "empty_table", geom_column="geom")
            assert bounds is None


def test_get_table_bounds_uses_quoted_identifiers(postgis_resource):
    """Test that get_table_bounds uses escaped identifiers."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.table_exists') as mock_exists:
        with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
            mock_exists.return_value = True

            mock_engine = MagicMock()
            mock_conn = MagicMock()

            # Mock column check result (geometry column exists)
            mock_column_result = MagicMock()
            mock_column_result.scalar.return_value = True

            mock_extent_result = MagicMock()
            mock_extent_result.fetchone.return_value = (0.0, 0.0, 1.0, 1.0)
            mock_conn.execute.side_effect = [mock_column_result, mock_extent_result]

            mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)

            mock_get_engine.return_value = mock_engine

            # Should execute without error and return bounds
            bounds = postgis_resource.get_table_bounds("proc_abc123", "my_table", geom_column="geom")

            # Verify it returned a Bounds object
            assert isinstance(bounds, Bounds)
            assert mock_conn.execute.called


def test_get_table_bounds_rejects_invalid_geom_column_names(postgis_resource):
    """Test that get_table_bounds rejects invalid geometry column names."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.table_exists') as mock_exists:
        mock_exists.return_value = True

        # Test various invalid column names
        invalid_names = [
            "geom-name",  # hyphens not allowed
            "geom;drop table",  # semicolons not allowed
            "123geom",  # cannot start with number
            "",  # empty string
            "geom name",  # spaces not allowed
            "geom.name",  # dots not allowed
        ]

        for invalid_name in invalid_names:
            with pytest.raises(ValueError, match="Invalid geometry column name"):
                postgis_resource.get_table_bounds("proc_abc123", "my_table", geom_column=invalid_name)


def test_get_table_bounds_accepts_valid_geom_column_names(postgis_resource):
    """Test that get_table_bounds accepts valid geometry column names."""
    with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.table_exists') as mock_exists:
        with patch('services.dagster.etl_pipelines.resources.postgis_resource.PostGISResource.get_engine') as mock_get_engine:
            mock_exists.return_value = True

            mock_engine = MagicMock()
            mock_conn = MagicMock()

            # Mock column check result (geometry column exists)
            mock_column_result = MagicMock()
            mock_column_result.scalar.return_value = True

            mock_extent_result = MagicMock()
            mock_extent_result.fetchone.return_value = (0.0, 0.0, 1.0, 1.0)
            mock_conn.execute.side_effect = [mock_column_result, mock_extent_result]

            mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)

            mock_get_engine.return_value = mock_engine

            # Test various valid column names
            valid_names = [
                "geom",
                "geometry",
                "geom_col",
                "my_geom_123",
                "_geom",
                "G",
            ]

            for valid_name in valid_names:
                # Should not raise an exception for valid names
                try:
                    postgis_resource.get_table_bounds("proc_abc123", "my_table", geom_column=valid_name)
                except Exception as e:
                    if "Invalid geometry column name" in str(e):
                        pytest.fail(f"Valid column name '{valid_name}' was rejected: {e}")
