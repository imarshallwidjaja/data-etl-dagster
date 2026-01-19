"""MongoDB Resource - Metadata ledger operations."""

from __future__ import annotations

from datetime import datetime, timezone
from functools import cached_property
from typing import Any, ClassVar, Dict

from dagster import ConfigurableResource
from pydantic import Field
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from bson import ObjectId

from pymongo.errors import OperationFailure

from libs.models import Asset, ManifestRecord, ManifestStatus, Run, RunStatus

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
    LINEAGE: ClassVar[str] = "lineage"
    RUNS: ClassVar[str] = "runs"

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
        if status == ManifestStatus.SUCCESS:
            update_doc["completed_at"] = now
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
    # Run operations
    # ------------------------------------------------------------------

    def insert_run(
        self,
        dagster_run_id: str,
        batch_id: str,
        job_name: str,
        partition_key: str | None = None,
        status: RunStatus = RunStatus.RUNNING,
    ) -> str:
        """
        Create or update a run document (upsert by dagster_run_id).

        Returns the ObjectId of the run document as a string.
        Upsert ensures idempotency if called multiple times for the same run.
        """
        collection = self._get_collection(self.RUNS)
        now = datetime.now(timezone.utc)
        run_doc = {
            "dagster_run_id": dagster_run_id,
            "batch_id": batch_id,
            "job_name": job_name,
            "partition_key": partition_key,
            "status": status.value,
            "asset_ids": [],
            "started_at": now,
            "completed_at": None,
            "error_message": None,
        }
        result = collection.update_one(
            {"dagster_run_id": dagster_run_id},
            {"$setOnInsert": run_doc},
            upsert=True,
        )
        if result.upserted_id:
            return str(result.upserted_id)
        # Document existed, return its ObjectId
        existing = collection.find_one(
            {"dagster_run_id": dagster_run_id}, projection={"_id": 1}
        )
        return str(existing["_id"]) if existing else ""

    def update_run_status(
        self,
        dagster_run_id: str,
        status: RunStatus,
        *,
        error_message: str | None = None,
    ) -> None:
        """
        Update the status of an existing run document.
        """
        collection = self._get_collection(self.RUNS)
        now = datetime.now(timezone.utc)
        update_doc: Dict[str, Any] = {
            "status": status.value,
        }
        if status in (RunStatus.SUCCESS, RunStatus.FAILURE, RunStatus.CANCELED):
            update_doc["completed_at"] = now
        if error_message:
            update_doc["error_message"] = error_message

        collection.update_one({"dagster_run_id": dagster_run_id}, {"$set": update_doc})

    def get_run(self, dagster_run_id: str) -> Run | None:
        """
        Load a run document by dagster_run_id.
        """
        collection = self._get_collection(self.RUNS)
        document = collection.find_one({"dagster_run_id": dagster_run_id})
        if not document:
            return None
        return Run(**self._strip_object_id(document))

    def get_run_object_id(self, dagster_run_id: str) -> str | None:
        """
        Return the MongoDB ObjectId (as string) for a run by dagster_run_id.

        Useful for linking assets to their run document.
        """
        collection = self._get_collection(self.RUNS)
        document = collection.find_one(
            {"dagster_run_id": dagster_run_id}, projection={"_id": 1}
        )
        if not document:
            return None
        return str(document["_id"])

    def add_asset_to_run(self, dagster_run_id: str, asset_id: str) -> None:
        """
        Add an asset ObjectId to the run's asset_ids list.
        """
        collection = self._get_collection(self.RUNS)
        collection.update_one(
            {"dagster_run_id": dagster_run_id},
            {"$addToSet": {"asset_ids": ObjectId(asset_id)}},
        )

    # ------------------------------------------------------------------
    # Asset operations
    # ------------------------------------------------------------------

    def insert_asset(self, asset: Asset) -> str:
        """
        Register a processed asset in MongoDB.

        Note: Uses model_dump() WITHOUT mode="json" to preserve datetime objects
        as native Python datetime, which pymongo converts to BSON date type.
        Using mode="json" would convert datetime to ISO strings, causing
        MongoDB schema validation to fail (bsonType: "date" requires actual dates).
        """
        collection = self._get_collection(self.ASSETS)
        result = collection.insert_one(asset.model_dump())
        return str(result.inserted_id)

    def get_asset_by_id(self, asset_id: str) -> Asset | None:
        """
        Retrieve an asset by its MongoDB ObjectId.

        Notes:
        - The returned Asset model does not include the MongoDB _id field.
          Use the input asset_id when you need to reference the document.
        """
        collection = self._get_collection(self.ASSETS)
        try:
            oid = ObjectId(asset_id)
        except Exception:
            return None

        document = collection.find_one({"_id": oid})
        if not document:
            return None
        return Asset(**self._strip_object_id(document))

    def insert_lineage(
        self,
        *,
        source_asset_id: str,
        target_asset_id: str,
        run_id: str,
        transformation: str,
        parameters: dict[str, Any] | None = None,
    ) -> str:
        """
        Record a lineage edge between two assets.

        Stores ObjectId references to source/target asset documents and run.
        """
        collection = self._get_collection(self.LINEAGE)
        document = {
            "source_asset_id": ObjectId(source_asset_id),
            "target_asset_id": ObjectId(target_asset_id),
            "run_id": ObjectId(run_id),
            "transformation": transformation,
            "parameters": parameters or {},
            "created_at": datetime.now(timezone.utc),
        }
        result = collection.insert_one(document)
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

    def get_asset_object_id(self, dataset_id: str) -> str | None:
        """
        Return the MongoDB ObjectId (as string) for the latest version of a dataset.

        Useful for lineage recording where the actual document _id is needed.
        """
        collection = self._get_collection(self.ASSETS)
        document = collection.find_one(
            {"dataset_id": dataset_id},
            sort=[("version", -1)],
            projection={"_id": 1},
        )
        if not document:
            return None
        return str(document["_id"])

    def get_asset_object_id_for_version(
        self, dataset_id: str, version: int
    ) -> str | None:
        """
        Return the MongoDB ObjectId (as string) for a specific dataset version.

        Useful for lineage recording when the exact version is known.
        """
        collection = self._get_collection(self.ASSETS)
        document = collection.find_one(
            {"dataset_id": dataset_id, "version": version},
            projection={"_id": 1},
        )
        if not document:
            return None
        return str(document["_id"])

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

    def search_assets_by_keywords(
        self,
        search_text: str,
        kind: str | None = None,
        limit: int = 20,
    ) -> list[Asset]:
        """
        Search assets by keywords using MongoDB text search.

        Uses text index on metadata.keywords (created by migration 002).

        Text search supports:
        - Multiple words (OR): "climate water" matches either term
        - Phrases: '"climate change"' matches exact phrase
        - Exclusions: "climate -water" excludes water

        Args:
            search_text: Text to search for in keywords.
            kind: Optional filter by asset kind.
            limit: Maximum results (default 20).

        Returns:
            List of matching Asset models, ordered by relevance.

        Example:
            >>> mongo.search_assets_by_keywords("population census")
            [Asset(dataset_id="sa2_population", ...), ...]
        """
        query: dict = {"$text": {"$search": search_text}}
        if kind:
            query["kind"] = kind

        try:
            cursor = (
                self._get_collection(self.ASSETS)
                .find(query, {"score": {"$meta": "textScore"}})
                .sort([("score", {"$meta": "textScore"})])
                .limit(limit)
            )
        except OperationFailure as e:
            if "text index required" in str(e).lower():
                raise RuntimeError(
                    "Text search index not found. "
                    "Ensure migration 002_add_text_search has been applied."
                ) from e
            raise

        assets = []
        for doc in cursor:
            doc = self._strip_object_id(doc)
            doc.pop("score", None)
            assets.append(Asset.model_validate(doc))

        return assets
