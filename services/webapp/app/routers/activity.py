import json
from pathlib import Path
from typing import Optional, Any

from fastapi import APIRouter, Depends, Query, Request
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.auth.dependencies import AuthenticatedUser, get_current_user
from app.services.activity_service import get_activity_service

router = APIRouter(prefix="/activity", tags=["activity"])

# Templates
BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


class ActivityLogItem(BaseModel):
    id: str
    user: str
    action: str
    resource_type: str
    resource_id: str
    details: dict
    timestamp: str
    ip_address: Optional[str]


class ActivityListResponse(BaseModel):
    items: list[ActivityLogItem]
    count_returned: int
    offset: int
    limit: int
    filters: dict[str, Any]


@router.get("/", response_model=None)
async def list_activity(
    request: Request,
    user: Optional[str] = Query(None, description="Filter by user"),
    action: Optional[str] = Query(None, description="Filter by action"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    resource_id: Optional[str] = Query(None, description="Filter by resource ID"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    limit: int = Query(50, ge=1, le=100, description="Limit for pagination"),
    format: str = Query("html", description="Response format: html or json"),
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    """List activity logs."""
    service = get_activity_service()
    entries, count = service.get_recent_activity(
        user=user,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        offset=offset,
        limit=limit,
    )

    activity_items = [
        ActivityLogItem(
            id=entry.id,
            user=entry.user,
            action=entry.action,
            resource_type=entry.resource_type,
            resource_id=entry.resource_id,
            details=entry.details,
            timestamp=entry.timestamp.isoformat(),
            ip_address=entry.ip_address,
        )
        for entry in entries
    ]

    filters = {
        "user": user,
        "action": action,
        "resource_type": resource_type,
        "resource_id": resource_id,
    }

    if format == "json":
        return ActivityListResponse(
            items=activity_items,
            count_returned=count,
            offset=offset,
            limit=limit,
            filters=filters,
        )

    return templates.TemplateResponse(
        "activity/list.html",
        {
            "request": request,
            "user": current_user,
            "activities": activity_items,
            "filters": filters,
            "pagination": {
                "offset": offset,
                "limit": limit,
                "count": count,
                "next_offset": offset + limit if count == limit else None,
                "prev_offset": max(0, offset - limit) if offset > 0 else None,
            },
        },
    )
