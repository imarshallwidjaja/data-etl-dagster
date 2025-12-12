"""MongoDB Resource - Metadata ledger operations."""

from __future__ import annotations

from datetime import datetime, timezone
from functools import cached_property
from typing import ClassVar, Dict

from dagster import ConfigurableResource
from pydantic import Field
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from libs.models import Asset, ManifestRecord, ManifestStatus

__all__ = ["MongoDBResource"]


class MongoDBResource(ConfigurableResource):
    """
    Dagster resource for MongoDB metadata ledger operations.

    This resource is the Source of Truth for manifests and assets. It keeps
    all MongoDB interactions centralized so that ops can remain lightweight.
    """

    connection_string: str = Field(..., description="MongoDB connection URI")
    database: str = Field("spatial_etl", description="MongoDB database name")

    MANIFESTS: ClassVar[str] = "manifests"
    ASSETS: ClassVar[str] = "assets"

    @cached_property
    def _client(self) -> MongoClient:
        return MongoClient(self.connection_string)

    def _get_db(self) -> Database:
        return self._client[self.database]

    def _get_collection(self, name: str) -> Collection:
        return self._get_db()[name]

    @staticmethod
    def _strip_object_id(doc: Dict) -> Dict:
        stripped = dict(doc)
        stripped.pop("_id", None)
        return stripped

    # ------------------------------------------------------------------
    # Manifest operations
    # ------------------------------------------------------------------

    def insert_manifest(self, record: ManifestRecord) -> str:
        """
        Persist a manifest record and return the inserted document id.
        """
        collection = self._get_collection(self.MANIFESTS)
        result = collection.insert_one(record.model_dump())
        return str(result.inserted_id)

    def update_manifest_status(
        self,
        batch_id: str,
        status: ManifestStatus,
        *,
        dagster_run_id: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """
        Update the status and optional metadata for an existing manifest.
        """
        collection = self._get_collection(self.MANIFESTS)
        now = datetime.now(timezone.utc)
        update_doc = {
            "status": status.value,
            "updated_at": now,
        }
        if status == ManifestStatus.COMPLETED:
            update_doc["completed_at"] = now
        if dagster_run_id:
            update_doc["dagster_run_id"] = dagster_run_id
        if error_message:
            update_doc["error_message"] = error_message

        collection.update_one({"batch_id": batch_id}, {"$set": update_doc})

    def get_manifest(self, batch_id: str) -> ManifestRecord | None:
        """
        Load a manifest record by batch_id.
        """
        collection = self._get_collection(self.MANIFESTS)
        document = collection.find_one({"batch_id": batch_id})
        if not document:
            return None
        return ManifestRecord(**self._strip_object_id(document))

    # ------------------------------------------------------------------
    # Asset operations
    # ------------------------------------------------------------------

    def insert_asset(self, asset: Asset) -> str:
        """
        Register a processed asset in MongoDB.
        """
        collection = self._get_collection(self.ASSETS)
        result = collection.insert_one(asset.model_dump())
        return str(result.inserted_id)

    def get_asset(self, dataset_id: str, version: int) -> Asset | None:
        """
        Retrieve a specific asset version.
        """
        collection = self._get_collection(self.ASSETS)
        document = collection.find_one({"dataset_id": dataset_id, "version": version})
        if not document:
            return None
        return Asset(**self._strip_object_id(document))

    def get_latest_asset(self, dataset_id: str) -> Asset | None:
        """
        Return the latest asset for a dataset (highest version).
        """
        collection = self._get_collection(self.ASSETS)
        document = collection.find_one(
            {"dataset_id": dataset_id},
            sort=[("version", -1)],
        )
        if not document:
            return None
        return Asset(**self._strip_object_id(document))

    def asset_exists(self, content_hash: str) -> bool:
        """
        Check for an existing asset with the given content hash.
        """
        collection = self._get_collection(self.ASSETS)
        return collection.count_documents({"content_hash": content_hash}) > 0

    def get_next_version(self, dataset_id: str) -> int:
        """
        Calculate the next version number for a dataset.
        """
        latest = self.get_latest_asset(dataset_id)
        if not latest:
            return 1
        return latest.version + 1
