"""
Migration 002: Add Text Search Index

Adds text index on metadata.keywords for full-text search.
Enables MongoDBResource.search_assets_by_keywords() method.

Text search supports:
- Multiple words (OR): "climate water" matches either term
- Phrases: '"climate change"' matches exact phrase
- Exclusions: "climate -water" excludes water

Note: Uses default_language="english" for stemming.
For exact matches on technical codes, use the standard multikey index.
"""

from pymongo.database import Database

VERSION = "002"


def up(db: Database) -> None:
    """Add text search index."""
    if "assets" not in db.list_collection_names():
        return

    existing = {idx["name"] for idx in db.assets.list_indexes()}
    if "metadata_keywords_text" not in existing:
        db.assets.create_index(
            [("metadata.keywords", "text")],
            name="metadata_keywords_text",
            default_language="english",
        )


def down(db: Database) -> None:
    """Remove text search index."""
    if "assets" not in db.list_collection_names():
        return
    existing = {idx["name"] for idx in db.assets.list_indexes()}
    if "metadata_keywords_text" in existing:
        db.assets.drop_index("metadata_keywords_text")
