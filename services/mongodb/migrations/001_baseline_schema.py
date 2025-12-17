"""
Migration 001: Baseline Schema

Creates the baseline MongoDB schema with collections, validators, and indexes.
This migration establishes the core collections: assets, manifests, runs, and lineage.

Idempotency: Uses create_collection wrapped in try/except for CollectionInvalid,
then applies collMod to update validators unconditionally.
"""
from pymongo.database import Database
from pymongo.errors import CollectionInvalid

VERSION = "001"


def up(db: Database) -> None:
    """
    Apply this migration.
    
    Args:
        db: PyMongo Database instance (already connected).
        
    Raises:
        Exception: If migration fails (will cause retry on next startup).
    """
    # Assets Collection - Core asset registry
    assets_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["s3_key", "dataset_id", "version", "content_hash", "dagster_run_id", "kind", "format", "created_at"],
            "properties": {
                "s3_key": {
                    "bsonType": "string",
                    "description": "S3 object key - required"
                },
                "dataset_id": {
                    "bsonType": "string",
                    "description": "Unique dataset identifier - required"
                },
                "version": {
                    "bsonType": "int",
                    "minimum": 1,
                    "description": "Asset version number - required"
                },
                "content_hash": {
                    "bsonType": "string",
                    "pattern": "^sha256:[a-f0-9]{64}$",
                    "description": "SHA256 hash of content - required"
                },
                "dagster_run_id": {
                    "bsonType": "string",
                    "description": "Dagster run ID that created this asset - required"
                },
                "kind": {
                    "enum": ["spatial", "tabular", "joined"],
                    "description": "Asset kind - required"
                },
                "format": {
                    "enum": ["geoparquet", "cog", "geojson", "parquet"],
                    "description": "Output format - required"
                },
                "crs": {
                    "bsonType": ["string", "null"],
                    "description": "Coordinate Reference System (required for spatial/joined, null for tabular)"
                },
                "bounds": {
                    "bsonType": ["object", "null"],
                    "required": ["minx", "miny", "maxx", "maxy"],
                    "properties": {
                        "minx": {"bsonType": "double"},
                        "miny": {"bsonType": "double"},
                        "maxx": {"bsonType": "double"},
                        "maxy": {"bsonType": "double"}
                    }
                },
                "metadata": {
                    "bsonType": "object",
                    "required": ["title"],
                    "properties": {
                        "title": {"bsonType": "string"},
                        "description": {"bsonType": "string"},
                        "source": {"bsonType": "string"},
                        "license": {"bsonType": "string"},
                        "tags": {
                            "bsonType": "object",
                            "additionalProperties": {
                                "bsonType": ["string", "int", "long", "double", "bool"]
                            }
                        },
                        "header_mapping": {
                            "bsonType": ["object", "null"],
                            "additionalProperties": {"bsonType": "string"}
                        }
                    }
                },
                "created_at": {
                    "bsonType": "date",
                    "description": "Creation timestamp - required"
                },
                "updated_at": {
                    "bsonType": "date",
                    "description": "Last update timestamp"
                }
            }
        }
    }
    
    try:
        db.create_collection("assets", validator=assets_validator, validationLevel="strict", validationAction="error")
    except CollectionInvalid:
        # Collection exists, update validator
        db.command("collMod", "assets", validator=assets_validator, validationLevel="strict", validationAction="error")
    
    # Manifests Collection - Ingested manifest records
    manifests_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["batch_id", "uploader", "intent", "files", "metadata", "status", "ingested_at"],
            "properties": {
                "batch_id": {
                    "bsonType": "string",
                    "description": "Unique batch identifier - required"
                },
                "uploader": {
                    "bsonType": "string",
                    "description": "User or system that uploaded - required"
                },
                "intent": {
                    "bsonType": "string",
                    "description": "Processing intent - required"
                },
                "files": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "object",
                        "required": ["path", "type", "format"],
                        "properties": {
                            "path": {"bsonType": "string"},
                            "type": {"enum": ["raster", "vector", "tabular"]},
                            "format": {"bsonType": "string"}
                        },
                        "additionalProperties": False
                    }
                },
                "metadata": {
                    "bsonType": "object",
                    "required": ["project"],
                    "properties": {
                        "project": {"bsonType": "string"},
                        "description": {"bsonType": "string"},
                        "tags": {
                            "bsonType": "object",
                            "additionalProperties": {
                                "bsonType": ["string", "int", "long", "double", "bool"]
                            }
                        },
                        "join_config": {
                            "bsonType": "object",
                            "required": ["left_key"],
                            "additionalProperties": False,
                            "properties": {
                                "target_asset_id": {"bsonType": "string"},
                                "left_key": {"bsonType": "string"},
                                "right_key": {"bsonType": "string"},
                                "how": {"enum": ["left", "inner", "right", "outer"]}
                            }
                        }
                    },
                    "additionalProperties": False
                },
                "status": {
                    "enum": ["pending", "processing", "completed", "failed"],
                    "description": "Processing status - required"
                },
                "dagster_run_id": {
                    "bsonType": "string"
                },
                "error_message": {
                    "bsonType": "string"
                },
                "ingested_at": {
                    "bsonType": "date",
                    "description": "Ingestion timestamp - required"
                },
                "completed_at": {
                    "bsonType": "date"
                }
            }
        }
    }
    
    try:
        db.create_collection("manifests", validator=manifests_validator, validationLevel="strict", validationAction="error")
    except CollectionInvalid:
        # Collection exists, update validator
        db.command("collMod", "manifests", validator=manifests_validator, validationLevel="strict", validationAction="error")
    
    # Runs Collection - ETL run metadata
    runs_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["dagster_run_id", "manifest_id", "status", "started_at"],
            "properties": {
                "dagster_run_id": {
                    "bsonType": "string",
                    "description": "Dagster run ID - required"
                },
                "manifest_id": {
                    "bsonType": "objectId",
                    "description": "Reference to manifest - required"
                },
                "status": {
                    "enum": ["running", "success", "failure"],
                    "description": "Run status - required"
                },
                "assets_created": {
                    "bsonType": "array",
                    "items": {"bsonType": "objectId"}
                },
                "started_at": {
                    "bsonType": "date",
                    "description": "Run start time - required"
                },
                "completed_at": {
                    "bsonType": "date"
                },
                "error_message": {
                    "bsonType": "string"
                }
            }
        }
    }
    
    try:
        db.create_collection("runs", validator=runs_validator)
    except CollectionInvalid:
        # Collection exists, update validator
        db.command("collMod", "runs", validator=runs_validator)
    
    # Lineage Collection - Asset transformation graph
    lineage_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["source_asset_id", "target_asset_id", "dagster_run_id", "transformation", "created_at"],
            "properties": {
                "source_asset_id": {
                    "bsonType": "objectId",
                    "description": "Source asset reference - required"
                },
                "target_asset_id": {
                    "bsonType": "objectId",
                    "description": "Target asset reference - required"
                },
                "dagster_run_id": {
                    "bsonType": "string",
                    "description": "Run that created this relationship - required"
                },
                "transformation": {
                    "bsonType": "string",
                    "description": "Type of transformation applied - required"
                },
                "parameters": {
                    "bsonType": "object",
                    "description": "Transformation parameters"
                },
                "created_at": {
                    "bsonType": "date",
                    "description": "Lineage record creation time - required"
                }
            }
        }
    }
    
    try:
        db.create_collection("lineage", validator=lineage_validator)
    except CollectionInvalid:
        # Collection exists, update validator
        db.command("collMod", "lineage", validator=lineage_validator)
    
    # Create Indexes
    # Assets indexes
    db.assets.create_index([("s3_key", 1)], unique=True)
    db.assets.create_index([("dataset_id", 1), ("version", -1)])
    db.assets.create_index([("content_hash", 1)])
    db.assets.create_index([("dagster_run_id", 1)])
    db.assets.create_index([("kind", 1)])
    db.assets.create_index([("created_at", -1)])
    
    # Manifests indexes
    db.manifests.create_index([("batch_id", 1)], unique=True)
    db.manifests.create_index([("status", 1)])
    db.manifests.create_index([("ingested_at", -1)])
    
    # Runs indexes
    db.runs.create_index([("dagster_run_id", 1)], unique=True)
    db.runs.create_index([("manifest_id", 1)])
    db.runs.create_index([("status", 1)])
    
    # Lineage indexes
    db.lineage.create_index([("source_asset_id", 1)])
    db.lineage.create_index([("target_asset_id", 1)])
    db.lineage.create_index([("dagster_run_id", 1)])

