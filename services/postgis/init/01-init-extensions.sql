-- =============================================================================
-- PostGIS Initialization Script
-- =============================================================================
-- This script runs on first container startup to initialize PostGIS extensions
-- and create any necessary utility functions.
-- =============================================================================

-- Enable PostGIS extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- Enable UUID generation (useful for run IDs)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------------------------------------------------------
-- Utility Functions for Ephemeral Schema Management
-- -----------------------------------------------------------------------------

-- Function to create a processing schema with proper permissions
CREATE OR REPLACE FUNCTION create_processing_schema(run_id TEXT)
RETURNS TEXT AS $$
DECLARE
    schema_name TEXT;
BEGIN
    schema_name := 'proc_' || replace(run_id, '-', '_');
    
    -- Create the schema
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema_name);
    
    -- Grant usage to current user
    EXECUTE format('GRANT ALL ON SCHEMA %I TO CURRENT_USER', schema_name);
    
    RETURN schema_name;
END;
$$ LANGUAGE plpgsql;

-- Function to drop a processing schema (with CASCADE)
CREATE OR REPLACE FUNCTION drop_processing_schema(run_id TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    schema_name TEXT;
BEGIN
    schema_name := 'proc_' || replace(run_id, '-', '_');
    
    -- Drop the schema and all its contents
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', schema_name);
    
    RETURN TRUE;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Failed to drop schema %: %', schema_name, SQLERRM;
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

-- Function to list all processing schemas (for monitoring/cleanup)
CREATE OR REPLACE FUNCTION list_processing_schemas()
RETURNS TABLE(schema_name TEXT, created_at TIMESTAMP) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.schema_name::TEXT,
        NULL::TIMESTAMP as created_at  -- PostgreSQL doesn't track schema creation time
    FROM information_schema.schemata s
    WHERE s.schema_name LIKE 'proc_%'
    ORDER BY s.schema_name;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up all orphaned processing schemas (emergency cleanup)
CREATE OR REPLACE FUNCTION cleanup_all_processing_schemas()
RETURNS INTEGER AS $$
DECLARE
    schema_rec RECORD;
    dropped_count INTEGER := 0;
BEGIN
    FOR schema_rec IN 
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name LIKE 'proc_%'
    LOOP
        EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', schema_rec.schema_name);
        dropped_count := dropped_count + 1;
    END LOOP;
    
    RETURN dropped_count;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- Comments
-- -----------------------------------------------------------------------------

COMMENT ON FUNCTION create_processing_schema(TEXT) IS 
    'Creates a temporary processing schema for a Dagster run. Schema name is proc_{run_id}.';

COMMENT ON FUNCTION drop_processing_schema(TEXT) IS 
    'Drops a processing schema and all its contents. Should be called in finally block.';

COMMENT ON FUNCTION list_processing_schemas() IS 
    'Lists all processing schemas currently in the database. Useful for monitoring.';

COMMENT ON FUNCTION cleanup_all_processing_schemas() IS 
    'Emergency cleanup function to drop ALL processing schemas. Use with caution!';

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'PostGIS initialization completed successfully';
END $$;

