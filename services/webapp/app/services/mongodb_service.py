# =============================================================================
# MongoDB Service - Metadata Ledger Operations
# =============================================================================
# Service wrapper for MongoDB operations in the webapp.
# =============================================================================

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from app.config import get_settings


@dataclass
class ManifestSummary:
    """Summary of a manifest for list views."""

    batch_id: str
    intent: str
    status: str
    uploader: str
    project: str
    file_count: int
    created_at: datetime
    dagster_run_id: Optional[str] = None


@dataclass
class AssetSummary:
    """Summary of an asset for list views."""

    id: str  # MongoDB ObjectId as string
    dataset_id: str
    version: int
    kind: str
    s3_key: str
    created_at: datetime
    project: Optional[str] = None


@dataclass
class LineageEdge:
    """Represents a lineage relationship."""

    source_asset_id: str
    target_asset_id: str
    transformation: str
    parameters: dict
    created_at: datetime


class MongoDBService:
    """Service for MongoDB operations."""

    MANIFESTS = "manifests"
    ASSETS = "assets"
    LINEAGE = "lineage"

    def __init__(self) -> None:
        settings = get_settings()
        self._client = MongoClient(settings.mongo_connection_string)
        # Extract database name from connection string or use default
        self._db_name = self._extract_db_name(settings.mongo_connection_string)
        self._db: Database = self._client[self._db_name]

    @staticmethod
    def _extract_db_name(connection_string: str) -> str:
        """Extract database name from MongoDB connection string."""
        # Pattern: mongodb://.../<database>?...
        match = re.search(r"/([^/?]+)(\?|$)", connection_string.split("@")[-1])
        if match:
            return match.group(1)
        return "spatial_etl"  # Default

    def _get_collection(self, name: str) -> Collection:
        return self._db[name]

    # ------------------------------------------------------------------
    # Manifest operations
    # ------------------------------------------------------------------

    def list_manifests(
        self, status: Optional[str] = None, limit: int = 50
    ) -> list[ManifestSummary]:
        """
        List manifests with optional status filter.

        Args:
            status: Optional status filter (pending, processing, completed, failed)
            limit: Maximum number of results

        Returns:
            List of ManifestSummary
        """
        collection = self._get_collection(self.MANIFESTS)

        query = {}
        if status:
            query["status"] = status

        cursor = collection.find(query).sort("ingested_at", -1).limit(limit)

        results = []
        for doc in cursor:
            results.append(
                ManifestSummary(
                    batch_id=doc.get("batch_id", ""),
                    intent=doc.get("intent", ""),
                    status=doc.get("status", "unknown"),
                    uploader=doc.get("uploader", ""),
                    project=doc.get("metadata", {}).get("project", ""),
                    file_count=len(doc.get("files", [])),
                    created_at=doc.get("ingested_at") or datetime.now(),
                    dagster_run_id=doc.get("dagster_run_id"),
                )
            )

        return results

    def get_manifest(self, batch_id: str) -> Optional[dict]:
        """
        Get full manifest document by batch_id.

        Args:
            batch_id: Manifest batch ID

        Returns:
            Full manifest document or None
        """
        collection = self._get_collection(self.MANIFESTS)
        doc = collection.find_one({"batch_id": batch_id})
        if doc:
            doc["_id"] = str(doc["_id"])
        return doc

    # ------------------------------------------------------------------
    # Asset operations
    # ------------------------------------------------------------------

    def list_assets(
        self, kind: Optional[str] = None, limit: int = 50
    ) -> list[AssetSummary]:
        """
        List assets with optional kind filter.

        Args:
            kind: Optional kind filter (spatial, tabular, joined)
            limit: Maximum number of results

        Returns:
            List of AssetSummary
        """
        collection = self._get_collection(self.ASSETS)

        query = {}
        if kind:
            query["kind"] = kind

        cursor = collection.find(query).sort("created_at", -1).limit(limit)

        results = []
        for doc in cursor:
            results.append(
                AssetSummary(
                    id=str(doc.get("_id", "")),
                    dataset_id=doc.get("dataset_id", ""),
                    version=doc.get("version", 1),
                    kind=doc.get("kind", ""),
                    s3_key=doc.get("s3_key", ""),
                    created_at=doc.get("created_at", datetime.now()),
                    project=doc.get("metadata", {}).get("project"),
                )
            )

        return results

    def get_asset(
        self, dataset_id: str, version: Optional[int] = None
    ) -> Optional[dict]:
        """
        Get asset by dataset_id and optional version.

        Args:
            dataset_id: Dataset identifier
            version: Optional version (defaults to latest)

        Returns:
            Full asset document or None
        """
        collection = self._get_collection(self.ASSETS)

        if version is not None:
            doc = collection.find_one({"dataset_id": dataset_id, "version": version})
        else:
            # Get latest version
            doc = collection.find_one(
                {"dataset_id": dataset_id},
                sort=[("version", -1)],
            )

        if doc:
            doc["_id"] = str(doc["_id"])

        return doc

    def get_asset_versions(self, dataset_id: str) -> list[dict]:
        """
        Get all versions of an asset.

        Args:
            dataset_id: Dataset identifier

        Returns:
            List of asset documents (all versions)
        """
        collection = self._get_collection(self.ASSETS)

        cursor = collection.find({"dataset_id": dataset_id}).sort("version", -1)

        results = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)

        return results

    # ------------------------------------------------------------------
    # Lineage operations
    # ------------------------------------------------------------------

    def get_lineage(self, target_asset_id: str) -> list[LineageEdge]:
        """
        Get parent assets for a given asset.

        Args:
            target_asset_id: MongoDB ObjectId of the target asset

        Returns:
            List of LineageEdge (parent relationships)
        """
        collection = self._get_collection(self.LINEAGE)

        try:
            target_oid = ObjectId(target_asset_id)
        except Exception:
            return []

        cursor = collection.find({"target_asset_id": target_oid})

        results = []
        for doc in cursor:
            results.append(
                LineageEdge(
                    source_asset_id=str(doc.get("source_asset_id", "")),
                    target_asset_id=str(doc.get("target_asset_id", "")),
                    transformation=doc.get("transformation", ""),
                    parameters=doc.get("parameters", {}),
                    created_at=doc.get("created_at", datetime.now()),
                )
            )

        return results

    def get_parent_assets(self, target_asset_id: str) -> list[dict]:
        """
        Get full parent asset documents for a given asset.

        Args:
            target_asset_id: MongoDB ObjectId of the target asset

        Returns:
            List of parent asset documents
        """
        lineage_edges = self.get_lineage(target_asset_id)
        assets_collection = self._get_collection(self.ASSETS)

        parents = []
        for edge in lineage_edges:
            try:
                source_oid = ObjectId(edge.source_asset_id)
                doc = assets_collection.find_one({"_id": source_oid})
                if doc:
                    doc["_id"] = str(doc["_id"])
                    doc["_lineage_transformation"] = edge.transformation
                    parents.append(doc)
            except Exception:
                continue

        return parents

    # ------------------------------------------------------------------
    # Re-run versioning
    # ------------------------------------------------------------------

    def get_next_rerun_version(self, batch_id: str) -> int:
        """
        Find the next available version number for a batch_id re-run.

        Searches for existing batch_ids matching the pattern:
        - batch_id_v2, batch_id_v3, etc.

        Args:
            batch_id: Original batch ID

        Returns:
            Next version number (2 if no existing versions)
        """
        collection = self._get_collection(self.MANIFESTS)

        # Strip existing version suffix if present
        base_id = re.sub(r"_v\d+$", "", batch_id)

        # Find all versions of this batch_id
        pattern = f"^{re.escape(base_id)}(_v\\d+)?$"
        cursor = collection.find(
            {"batch_id": {"$regex": pattern}},
            projection={"batch_id": 1},
        )

        max_version = 1
        for doc in cursor:
            existing_id = doc.get("batch_id", "")
            match = re.search(r"_v(\d+)$", existing_id)
            if match:
                version = int(match.group(1))
                max_version = max(max_version, version)
            elif existing_id == base_id:
                # Original exists, so at least v2 is needed
                max_version = max(max_version, 1)

        return max_version + 1


# Singleton instance
_mongodb_service: Optional[MongoDBService] = None


def get_mongodb_service() -> MongoDBService:
    """Get or create the MongoDB service singleton."""
    global _mongodb_service
    if _mongodb_service is None:
        _mongodb_service = MongoDBService()
    return _mongodb_service
