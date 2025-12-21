"""
Migration 003: Add runs collection and unify status values

This migration:
1. Creates the 'runs' collection with indexes for Dagster run tracking
2. Updates manifest status enum values (pending/processing/completed/failed -> running/success/failure/canceled)
3. Removes dagster_run_id field from manifests (runs are now tracked separately)
4. Updates asset schema to use run_id (ObjectId) instead of dagster_run_id (string)
5. Updates lineage schema to use run_id (ObjectId) instead of dagster_run_id (string)

Runs collection schema:
- dagster_run_id: unique identifier (string)
- batch_id: links to manifest (1 manifest → N runs)
- job_name: Dagster job name
- partition_key: optional partition key
- status: running/success/failure/canceled
- asset_ids: list of ObjectIds for assets produced
- error_message: optional error details
- started_at: timestamp
- completed_at: optional timestamp
"""

from pymongo.database import Database

VERSION = "003"


def up(db: Database) -> None:
    """
    Apply this migration.

    Args:
        db: PyMongo Database instance (already connected).

    Raises:
        Exception: If migration fails (will cause retry on next startup).
    """
    # ==========================================================================
    # Step 1: Create runs collection with indexes
    # ==========================================================================
    if "runs" not in db.list_collection_names():
        db.create_collection("runs")

    runs = db["runs"]

    # Drop existing indexes that might conflict (idempotent)
    # We need to recreate them with specific options
    existing_indexes = list(runs.list_indexes())
    existing_index_names = {idx["name"] for idx in existing_indexes}

    # Drop and recreate indexes to ensure correct options
    indexes_to_create = [
        ("dagster_run_id_1", "dagster_run_id", {"unique": True, "sparse": True}),
        ("batch_id_1", "batch_id", {}),
        ("status_1", "status", {}),
    ]

    for index_name, field, options in indexes_to_create:
        if index_name in existing_index_names:
            runs.drop_index(index_name)
        runs.create_index(field, name=index_name, **options)

    # Compound index for batch_id + started_at
    compound_index_name = "batch_id_1_started_at_-1"
    if compound_index_name in existing_index_names:
        runs.drop_index(compound_index_name)
    runs.create_index(
        [("batch_id", 1), ("started_at", -1)], name=compound_index_name
    )  # For getting latest run per manifest

    # Add validator for runs collection
    runs_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "dagster_run_id",
                "batch_id",
                "job_name",
                "status",
                "started_at",
            ],
            "properties": {
                "dagster_run_id": {"bsonType": "string"},
                "batch_id": {"bsonType": "string"},
                "job_name": {"bsonType": "string"},
                "partition_key": {"bsonType": ["string", "null"]},
                "status": {"enum": ["running", "success", "failure", "canceled"]},
                "asset_ids": {"bsonType": "array", "items": {"bsonType": "objectId"}},
                "error_message": {"bsonType": ["string", "null"]},
                "started_at": {"bsonType": "date"},
                "completed_at": {"bsonType": ["date", "null"]},
            },
        }
    }
    db.command(
        "collMod",
        "runs",
        validator=runs_validator,
        validationLevel="strict",
        validationAction="error",
    )

    # ==========================================================================
    # Step 2: Migrate manifest status values
    # ==========================================================================
    manifests = db["manifests"]

    # Update old status values to new unified values
    manifests.update_many({"status": "pending"}, {"$set": {"status": "running"}})
    manifests.update_many({"status": "processing"}, {"$set": {"status": "running"}})
    manifests.update_many({"status": "completed"}, {"$set": {"status": "success"}})
    manifests.update_many({"status": "failed"}, {"$set": {"status": "failure"}})

    # Remove dagster_run_id field from manifests (runs are tracked separately now)
    manifests.update_many({}, {"$unset": {"dagster_run_id": ""}})

    # Update manifests validator with new status values
    manifests_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "batch_id",
                "uploader",
                "intent",
                "files",
                "metadata",
                "status",
                "ingested_at",
            ],
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
                            "format": {"bsonType": "string"},
                        },
                        "additionalProperties": False,
                    },
                },
                "metadata": {
                    "bsonType": "object",
                    "required": ["project"],
                    "properties": {
                        "project": {"bsonType": "string"},
                        "description": {"bsonType": ["string", "null"]},
                        "tags": {
                            "bsonType": ["object", "null"],
                            "additionalProperties": {
                                "bsonType": ["string", "int", "long", "double", "bool"]
                            },
                        },
                        "join_config": {
                            "bsonType": ["object", "null"],
                            "additionalProperties": True,
                        },
                    },
                    "additionalProperties": False,
                },
                "status": {"enum": ["running", "success", "failure", "canceled"]},
                "error_message": {"bsonType": ["string", "null"]},
                "ingested_at": {"bsonType": "date"},
                "completed_at": {"bsonType": ["date", "null"]},
                "updated_at": {"bsonType": ["date", "null"]},
            },
        }
    }
    db.command(
        "collMod",
        "manifests",
        validator=manifests_validator,
        validationLevel="strict",
        validationAction="error",
    )

    # ==========================================================================
    # Step 3: Update assets schema to use run_id (ObjectId) instead of dagster_run_id
    # ==========================================================================
    assets_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "s3_key",
                "dataset_id",
                "version",
                "content_hash",
                "run_id",
                "kind",
                "format",
                "created_at",
            ],
            "properties": {
                "s3_key": {"bsonType": "string"},
                "dataset_id": {"bsonType": "string"},
                "version": {"bsonType": "int", "minimum": 1},
                "content_hash": {
                    "bsonType": "string",
                    "pattern": "^sha256:[a-f0-9]{64}$",
                },
                "run_id": {"bsonType": "string"},  # ObjectId as string
                "kind": {"enum": ["spatial", "tabular", "joined"]},
                "format": {"enum": ["geoparquet", "cog", "geojson", "parquet"]},
                "crs": {"bsonType": ["string", "null"]},
                "bounds": {
                    "bsonType": ["object", "null"],
                    "properties": {
                        "minx": {"bsonType": "double"},
                        "miny": {"bsonType": "double"},
                        "maxx": {"bsonType": "double"},
                        "maxy": {"bsonType": "double"},
                    },
                },
                "metadata": {
                    "bsonType": "object",
                    "required": ["title"],
                    "properties": {
                        "title": {"bsonType": "string"},
                        "description": {"bsonType": ["string", "null"]},
                        "source": {"bsonType": ["string", "null"]},
                        "license": {"bsonType": ["string", "null"]},
                        "tags": {
                            "bsonType": "object",
                            "additionalProperties": {
                                "bsonType": ["string", "int", "long", "double", "bool"]
                            },
                        },
                        "header_mapping": {
                            "bsonType": ["object", "null"],
                            "additionalProperties": {"bsonType": "string"},
                        },
                    },
                },
                "created_at": {"bsonType": "date"},
                "updated_at": {"bsonType": ["date", "null"]},
            },
        }
    }
    db.command(
        "collMod",
        "assets",
        validator=assets_validator,
        validationLevel="strict",
        validationAction="error",
    )

    # ==========================================================================
    # Step 4: Update lineage schema to use run_id (ObjectId) instead of dagster_run_id
    # ==========================================================================
    lineage_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "source_asset_id",
                "target_asset_id",
                "run_id",
                "transformation",
                "created_at",
            ],
            "properties": {
                "source_asset_id": {"bsonType": "objectId"},
                "target_asset_id": {"bsonType": "objectId"},
                "run_id": {"bsonType": "objectId"},
                "transformation": {"bsonType": "string"},
                "parameters": {"bsonType": "object"},
                "created_at": {"bsonType": "date"},
            },
        }
    }

    # Check if lineage collection exists before modifying
    if "lineage" in db.list_collection_names():
        db.command(
            "collMod",
            "lineage",
            validator=lineage_validator,
            validationLevel="strict",
            validationAction="error",
        )


def down(db: Database) -> None:
    """
    Rollback this migration.

    Note: This rollback is destructive - it drops the runs collection.
    Asset and lineage documents with run_id will not be converted back.

    Args:
        db: PyMongo Database instance (already connected).
    """
    # Drop runs collection
    if "runs" in db.list_collection_names():
        db["runs"].drop()

    # Revert manifest status values (best effort)
    manifests = db["manifests"]
    manifests.update_many({"status": "running"}, {"$set": {"status": "processing"}})
    manifests.update_many({"status": "success"}, {"$set": {"status": "completed"}})
    manifests.update_many({"status": "failure"}, {"$set": {"status": "failed"}})
    manifests.update_many({"status": "canceled"}, {"$set": {"status": "failed"}})
