// =============================================================================
// MongoDB Initialization Script
// =============================================================================
// This script runs on first container startup to initialize the database,
// create collections with JSON Schema validation, and set up indexes.
// =============================================================================

// Switch to the spatial_etl database
db = db.getSiblingDB('spatial_etl');

// -----------------------------------------------------------------------------
// Create Collections with JSON Schema Validation
// -----------------------------------------------------------------------------

// Assets Collection - Core asset registry
db.createCollection('assets', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['s3_key', 'dataset_id', 'version', 'content_hash', 'dagster_run_id', 'format', 'crs', 'created_at'],
      properties: {
        s3_key: {
          bsonType: 'string',
          description: 'S3 object key - required'
        },
        dataset_id: {
          bsonType: 'string',
          description: 'Unique dataset identifier - required'
        },
        version: {
          bsonType: 'int',
          minimum: 1,
          description: 'Asset version number - required'
        },
        content_hash: {
          bsonType: 'string',
          pattern: '^sha256:[a-f0-9]{64}$',
          description: 'SHA256 hash of content - required'
        },
        dagster_run_id: {
          bsonType: 'string',
          description: 'Dagster run ID that created this asset - required'
        },
        format: {
          enum: ['geoparquet', 'cog', 'geojson'],
          description: 'Output format - required'
        },
        crs: {
          bsonType: 'string',
          description: 'Coordinate Reference System - required'
        },
        bounds: {
          bsonType: 'object',
          required: ['minx', 'miny', 'maxx', 'maxy'],
          properties: {
            minx: { bsonType: 'double' },
            miny: { bsonType: 'double' },
            maxx: { bsonType: 'double' },
            maxy: { bsonType: 'double' }
          }
        },
        metadata: {
          bsonType: 'object',
          properties: {
            title: { bsonType: 'string' },
            description: { bsonType: 'string' },
            source: { bsonType: 'string' },
            license: { bsonType: 'string' }
          }
        },
        created_at: {
          bsonType: 'date',
          description: 'Creation timestamp - required'
        },
        updated_at: {
          bsonType: 'date',
          description: 'Last update timestamp'
        }
      }
    }
  },
  validationLevel: 'strict',
  validationAction: 'error'
});

// Manifests Collection - Ingested manifest records
db.createCollection('manifests', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['batch_id', 'uploader', 'intent', 'files', 'metadata', 'status', 'ingested_at'],
      properties: {
        batch_id: {
          bsonType: 'string',
          description: 'Unique batch identifier - required'
        },
        uploader: {
          bsonType: 'string',
          description: 'User or system that uploaded - required'
        },
        intent: {
          bsonType: 'string',
          description: 'Processing intent - required'
        },
        files: {
          bsonType: 'array',
          items: {
            bsonType: 'object',
            required: ['path', 'type', 'format'],
            properties: {
              path: { bsonType: 'string' },
              type: { enum: ['raster', 'vector'] },
              format: { bsonType: 'string' }
            }
          }
        },
        metadata: {
          bsonType: 'object',
          required: ['project'],
          properties: {
            project: { bsonType: 'string' },
            description: { bsonType: 'string' },
            tags: {
              bsonType: 'object',
              additionalProperties: {
                bsonType: ['string', 'int', 'double', 'bool']
              }
            },
            join_config: {
              bsonType: 'object',
              required: ['left_key'],
              additionalProperties: false,
              properties: {
                target_asset_id: { bsonType: 'string' },
                left_key: { bsonType: 'string' },
                right_key: { bsonType: 'string' },
                how: { enum: ['left', 'inner', 'right', 'outer'] }
              }
            }
          },
          additionalProperties: false
        },
        status: {
          enum: ['pending', 'processing', 'completed', 'failed'],
          description: 'Processing status - required'
        },
        dagster_run_id: {
          bsonType: 'string'
        },
        error_message: {
          bsonType: 'string'
        },
        ingested_at: {
          bsonType: 'date',
          description: 'Ingestion timestamp - required'
        },
        completed_at: {
          bsonType: 'date'
        }
      }
    }
  },
  validationLevel: 'strict',
  validationAction: 'error'
});

// Runs Collection - ETL run metadata
db.createCollection('runs', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['dagster_run_id', 'manifest_id', 'status', 'started_at'],
      properties: {
        dagster_run_id: {
          bsonType: 'string',
          description: 'Dagster run ID - required'
        },
        manifest_id: {
          bsonType: 'objectId',
          description: 'Reference to manifest - required'
        },
        status: {
          enum: ['running', 'success', 'failure'],
          description: 'Run status - required'
        },
        assets_created: {
          bsonType: 'array',
          items: { bsonType: 'objectId' }
        },
        started_at: {
          bsonType: 'date',
          description: 'Run start time - required'
        },
        completed_at: {
          bsonType: 'date'
        },
        error_message: {
          bsonType: 'string'
        }
      }
    }
  }
});

// Lineage Collection - Asset transformation graph
db.createCollection('lineage', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['source_asset_id', 'target_asset_id', 'dagster_run_id', 'transformation', 'created_at'],
      properties: {
        source_asset_id: {
          bsonType: 'objectId',
          description: 'Source asset reference - required'
        },
        target_asset_id: {
          bsonType: 'objectId',
          description: 'Target asset reference - required'
        },
        dagster_run_id: {
          bsonType: 'string',
          description: 'Run that created this relationship - required'
        },
        transformation: {
          bsonType: 'string',
          description: 'Type of transformation applied - required'
        },
        parameters: {
          bsonType: 'object',
          description: 'Transformation parameters'
        },
        created_at: {
          bsonType: 'date',
          description: 'Lineage record creation time - required'
        }
      }
    }
  }
});

// -----------------------------------------------------------------------------
// Create Indexes
// -----------------------------------------------------------------------------

// Assets indexes
db.assets.createIndex({ 's3_key': 1 }, { unique: true });
db.assets.createIndex({ 'dataset_id': 1, 'version': -1 });
db.assets.createIndex({ 'content_hash': 1 });
db.assets.createIndex({ 'dagster_run_id': 1 });
db.assets.createIndex({ 'created_at': -1 });

// Manifests indexes
db.manifests.createIndex({ 'batch_id': 1 }, { unique: true });
db.manifests.createIndex({ 'status': 1 });
db.manifests.createIndex({ 'ingested_at': -1 });

// Runs indexes
db.runs.createIndex({ 'dagster_run_id': 1 }, { unique: true });
db.runs.createIndex({ 'manifest_id': 1 });
db.runs.createIndex({ 'status': 1 });

// Lineage indexes
db.lineage.createIndex({ 'source_asset_id': 1 });
db.lineage.createIndex({ 'target_asset_id': 1 });
db.lineage.createIndex({ 'dagster_run_id': 1 });

print('MongoDB initialization completed successfully');

