# =============================================================================
# Activity Service - Audit Trail Operations
# =============================================================================
# Service for logging and querying user activity in the webapp.
# =============================================================================

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from pymongo import MongoClient, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from app.config import get_settings


@dataclass
class ActivityLogEntry:
    """Represents an activity log entry."""

    id: str  # MongoDB ObjectId as string
    user: str
    action: str
    resource_type: str
    resource_id: str
    details: dict
    timestamp: datetime
    ip_address: Optional[str] = None


class ActivityService:
    """Service for activity logging operations."""

    ACTIVITY_LOGS = "activity_logs"

    def __init__(self) -> None:
        settings = get_settings()
        self._client = MongoClient(settings.mongo_connection_string)
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

    def _get_collection(self) -> Collection:
        return self._db[self.ACTIVITY_LOGS]

    # ------------------------------------------------------------------
    # Activity logging
    # ------------------------------------------------------------------

    def log_activity(
        self,
        user: str,
        action: str,
        resource_type: str,
        resource_id: str,
        details: Optional[dict] = None,
        ip_address: Optional[str] = None,
    ) -> str:
        """
        Log a user activity.

        Args:
            user: Username or user identifier
            action: Action performed (e.g., 'upload', 'download', 'delete')
            resource_type: Type of resource (e.g., 'manifest', 'asset', 'file')
            resource_id: Identifier of the resource
            details: Optional additional details as a dictionary
            ip_address: Optional IP address of the user

        Returns:
            The inserted document's ObjectId as a string
        """
        collection = self._get_collection()

        doc: dict[str, Any] = {
            "user": user,
            "action": action,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "details": details or {},
            "timestamp": datetime.utcnow(),
        }

        if ip_address is not None:
            doc["ip_address"] = ip_address

        result = collection.insert_one(doc)
        return str(result.inserted_id)

    def get_recent_activity(
        self,
        user: Optional[str] = None,
        action: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        offset: int = 0,
        limit: int = 50,
    ) -> tuple[list[ActivityLogEntry], int]:
        """
        Get recent activity logs with optional filters and pagination.

        Args:
            user: Optional filter by user
            action: Optional filter by action
            resource_type: Optional filter by resource type
            resource_id: Optional filter by resource ID
            offset: Number of records to skip (default: 0)
            limit: Maximum number of records to return (default: 50)

        Returns:
            Tuple of (list of ActivityLogEntry, count of returned items)
        """
        collection = self._get_collection()

        # Build query with optional filters
        query: dict[str, Any] = {}
        if user is not None:
            query["user"] = user
        if action is not None:
            query["action"] = action
        if resource_type is not None:
            query["resource_type"] = resource_type
        if resource_id is not None:
            query["resource_id"] = resource_id

        cursor = (
            collection.find(query)
            .sort("timestamp", DESCENDING)
            .skip(offset)
            .limit(limit)
        )

        results: list[ActivityLogEntry] = []
        for doc in cursor:
            results.append(
                ActivityLogEntry(
                    id=str(doc.get("_id", "")),
                    user=doc.get("user", ""),
                    action=doc.get("action", ""),
                    resource_type=doc.get("resource_type", ""),
                    resource_id=doc.get("resource_id", ""),
                    details=doc.get("details", {}),
                    timestamp=doc.get("timestamp", datetime.utcnow()),
                    ip_address=doc.get("ip_address"),
                )
            )

        return results, len(results)


# Singleton instance
_activity_service: Optional[ActivityService] = None


def get_activity_service() -> ActivityService:
    """Get or create the ActivityService singleton."""
    global _activity_service
    if _activity_service is None:
        _activity_service = ActivityService()
    return _activity_service
