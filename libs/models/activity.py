# =============================================================================
# Activity Log Model
# =============================================================================
# Defines the ActivityLog model for tracking user/system actions in MongoDB.
# =============================================================================

from datetime import datetime, timezone
from typing import Literal, Any

from pydantic import BaseModel, Field


__all__ = ["ActivityLog", "ActivityAction", "ActivityResourceType"]


# Type aliases for literal types
ActivityAction = Literal[
    "create_manifest",
    "rerun_manifest",
    "delete_manifest",
    "upload_file",
    "delete_file",
    "download_asset",
    "archive_raw_source",
    "run_started",
    "run_success",
    "run_failure",
    "run_canceled",
    "login_success",
    "login_failure",
    "logout",
    "unauthorized_access",
]

ActivityResourceType = Literal["manifest", "file", "asset", "artifact", "run", "auth"]


class ActivityLog(BaseModel):
    """
    Activity log document model for MongoDB audit trail.

    Records user and system actions for audit, debugging, and analytics.
    Each log entry captures who did what, when, to which resource.

    Attributes:
        timestamp: When the action occurred (UTC)
        user: User or system identifier who performed the action
        action: Type of action performed (e.g., create_manifest, login_success)
        resource_type: Type of resource affected (manifest, file, asset, run, auth)
        resource_id: Identifier of the affected resource
        details: Additional context about the action (flexible dict)
        ip_address: Client IP address (for auth-related actions)
    """

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When the action occurred (UTC)",
    )
    user: str = Field(..., description="User or system identifier")
    action: ActivityAction = Field(..., description="Type of action performed")
    resource_type: ActivityResourceType = Field(
        ..., description="Type of resource affected"
    )
    resource_id: str = Field(..., description="Identifier of the affected resource")
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional context about the action",
    )
    ip_address: str | None = Field(
        default=None,
        description="Client IP address (for auth-related actions)",
    )
