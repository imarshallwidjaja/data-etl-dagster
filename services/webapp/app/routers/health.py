# =============================================================================
# Health Check Router
# =============================================================================
# Endpoints for container health checks and readiness probes.
# =============================================================================

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.auth.dependencies import AuthenticatedUser, get_current_user

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str
    version: str


class ReadyResponse(BaseModel):
    """Readiness check response model."""

    status: str
    services: dict[str, str]


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Basic health check endpoint.

    Returns 200 if the service is running.
    No authentication required for container health checks.
    """
    from app import __version__

    return HealthResponse(status="healthy", version=__version__)


@router.get("/ready", response_model=ReadyResponse)
async def readiness_check() -> ReadyResponse:
    """
    Readiness check endpoint.

    Verifies connectivity to dependent services.
    No authentication required for container readiness probes.

    TODO: Add actual service connectivity checks in Phase 2.
    """
    # Placeholder - will add actual checks when services are implemented
    return ReadyResponse(
        status="ready",
        services={
            "minio": "unchecked",
            "mongodb": "unchecked",
            "dagster": "unchecked",
        },
    )


@router.get("/whoami")
async def whoami(
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> dict:
    """
    Return the current authenticated user.

    Requires authentication - useful for testing auth flow.
    """
    return {
        "username": current_user.username,
        "display_name": current_user.display_name,
    }
