#!/usr/bin/env python
"""
Reset MongoDB collections for development.

Usage:
    python scripts/reset_mongodb.py --help
    python scripts/reset_mongodb.py --collections assets manifests --confirm
    python scripts/reset_mongodb.py --all --confirm

This utility helps developers recover from schema validation errors
when working with strict MongoDB validators.
"""

import argparse
import os
from pymongo import MongoClient


def get_mongo_client() -> MongoClient:
    """Get MongoDB client from environment."""
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    return MongoClient(uri)


def reset_collections(db, collections: list[str], confirm: bool) -> None:
    """Drop specified collections."""
    if not confirm:
        print("Dry run - would drop:", collections)
        print("Add --confirm to actually drop collections.")
        return

    for coll in collections:
        if coll in db.list_collection_names():
            db.drop_collection(coll)
            print(f"Dropped: {coll}")
        else:
            print(f"Skipped (not found): {coll}")


def main():
    parser = argparse.ArgumentParser(description="Reset MongoDB collections")
    parser.add_argument("--collections", nargs="+", help="Collections to drop")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Drop all ETL collections (assets, manifests, runs, lineage, schema_migrations)",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually perform the drop (without this, dry run only)",
    )
    parser.add_argument(
        "--db", default=os.getenv("MONGODB_DB", "etl_metadata"), help="Database name"
    )

    args = parser.parse_args()

    if args.all:
        collections = ["assets", "manifests", "runs", "lineage", "schema_migrations"]
    elif args.collections:
        collections = args.collections
    else:
        parser.print_help()
        return

    client = get_mongo_client()
    db = client[args.db]

    print(f"Target database: {args.db}")
    reset_collections(db, collections, args.confirm)


if __name__ == "__main__":
    main()
