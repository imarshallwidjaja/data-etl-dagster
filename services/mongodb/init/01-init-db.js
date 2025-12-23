// =============================================================================
// MongoDB Initialization Script (STUB)
// =============================================================================
// Schema setup is now handled by Python migrations in services/mongodb/migrations/
// This file remains only for backwards compatibility with Docker Compose.
// 
// See migrations/001_baseline_schema.py for current schema and indexes.
// =============================================================================

// Switch to the spatial_etl database to ensure it exists
db = db.getSiblingDB('spatial_etl');

print('MongoDB initialized - schema managed by migrations');
