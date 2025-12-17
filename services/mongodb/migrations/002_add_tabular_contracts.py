"""
Migration 002: Add Tabular Contracts

This migration documents the addition of tabular support to the schema.
However, since migration 001 already includes tabular support in the baseline
schema (kind: ['spatial', 'tabular', 'joined'], format: ['parquet'], etc.),
this migration is effectively a no-op for new installations.

For existing deployments that were initialized with an older version of 01-init-db.js
(without tabular support), this migration would update validators to allow:
- assets.kind: "tabular"
- assets.format: "parquet"
- assets.crs: null (for tabular assets)
- assets.bounds: null (for tabular assets)
- manifests.files[].type: "tabular"

Since the baseline already includes these, this migration serves as documentation
and ensures idempotency for any edge cases.
"""
from pymongo.database import Database
from pymongo.errors import CollectionInvalid

VERSION = "002"


def up(db: Database) -> None:
    """
    Apply this migration.
    
    This migration is a no-op since tabular support is already included
    in migration 001. It exists for documentation and to handle edge cases
    where an older baseline might not have included tabular support.
    
    Args:
        db: PyMongo Database instance (already connected).
        
    Raises:
        Exception: If migration fails (will cause retry on next startup).
    """
    # Verify that collections exist (they should from migration 001)
    collections = db.list_collection_names()
    
    if "assets" not in collections:
        raise RuntimeError("Migration 002 requires 'assets' collection from migration 001")
    
    if "manifests" not in collections:
        raise RuntimeError("Migration 002 requires 'manifests' collection from migration 001")
    
    # Get current validators to check if they already support tabular
    assets_info = db.command("listCollections", filter={"name": "assets"})
    first_batch = assets_info.get("cursor", {}).get("firstBatch", [])
    assets_coll_info = first_batch[0] if first_batch else None
    
    if assets_coll_info and "options" in assets_coll_info and "validator" in assets_coll_info["options"]:
        validator = assets_coll_info["options"]["validator"]["$jsonSchema"]
        
        # Check if tabular is already supported in kind enum
        kind_prop = validator.get("properties", {}).get("kind", {})
        if "enum" in kind_prop and "tabular" in kind_prop["enum"]:
            # Tabular support already present - this is a no-op
            return
    
    # If we reach here, the validator doesn't have tabular support
    # Update it to match migration 001's validator (with tabular support)
    # This handles edge cases where an older baseline was used
    
    assets_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["s3_key", "dataset_id", "version", "content_hash", "dagster_run_id", "kind", "format", "created_at"],
            "properties": {
                "s3_key": {"bsonType": "string"},
                "dataset_id": {"bsonType": "string"},
                "version": {"bsonType": "int", "minimum": 1},
                "content_hash": {"bsonType": "string", "pattern": "^sha256:[a-f0-9]{64}$"},
                "dagster_run_id": {"bsonType": "string"},
                "kind": {"enum": ["spatial", "tabular", "joined"]},
                "format": {"enum": ["geoparquet", "cog", "geojson", "parquet"]},
                "crs": {"bsonType": ["string", "null"]},
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
                "created_at": {"bsonType": "date"},
                "updated_at": {"bsonType": "date"}
            }
        }
    }
    
    manifests_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["batch_id", "uploader", "intent", "files", "metadata", "status", "ingested_at"],
            "properties": {
                "batch_id": {"bsonType": "string"},
                "uploader": {"bsonType": "string"},
                "intent": {"bsonType": "string"},
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
                "status": {"enum": ["pending", "processing", "completed", "failed"]},
                "dagster_run_id": {"bsonType": "string"},
                "error_message": {"bsonType": "string"},
                "ingested_at": {"bsonType": "date"},
                "completed_at": {"bsonType": "date"}
            }
        }
    }
    
    # Update validators (idempotent - safe to run even if already updated)
    db.command("collMod", "assets", validator=assets_validator, validationLevel="strict", validationAction="error")
    db.command("collMod", "manifests", validator=manifests_validator, validationLevel="strict", validationAction="error")

