# =============================================================================
# Authentication Dependencies
# =============================================================================
# FastAPI dependencies for authentication.
# Transport-agnostic: session-first, optional Basic fallback in hybrid mode.
# =============================================================================

import logging
from typing import Optional
from urllib.parse import quote

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.responses import Response

from app.auth.providers import AuthenticatedUser, BasicAuthProvider
from app.config import Settings, get_settings

logger = logging.getLogger(__name__)

# auto_error=False so missing Authorization header returns None
# instead of raising 401 automatically.
security = HTTPBasic(auto_error=False)


class _LoginRedirectException(Exception):
    """Raised when an unauthenticated browser request should redirect to /login."""

    def __init__(self, next_path: str) -> None:
        self.next_path = next_path


def login_redirect_handler(request: Request, exc: _LoginRedirectException) -> Response:
    """Convert _LoginRedirectException into a 303 redirect to /login."""
    return RedirectResponse(
        url=f"/login?next={exc.next_path}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


def wants_json(request: Request) -> bool:
    """
    Determine whether the client expects a JSON response.

    Returns True if:
    - ``format=json`` query parameter is present.
    - The request path is ``/whoami`` (API-only endpoint).
    - The ``Accept`` header contains ``application/json``.
    """
    if request.query_params.get("format") == "json":
        return True
    if request.url.path == "/whoami":
        return True
    accept = request.headers.get("accept", "")
    if "application/json" in accept:
        return True
    return False


def _get_client_ip(request: Request) -> Optional[str]:
    """Extract client IP from X-Forwarded-For header or request.client.host."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return None


def get_auth_provider(settings: Settings = Depends(get_settings)) -> BasicAuthProvider:
    """Get the authentication provider instance."""
    return BasicAuthProvider(
        username=settings.webapp_username,
        password=settings.webapp_password,
    )


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPBasicCredentials] = Depends(security),
    auth_provider: BasicAuthProvider = Depends(get_auth_provider),
    settings: Settings = Depends(get_settings),
) -> AuthenticatedUser:
    """
    Transport-agnostic authentication dependency.

    Resolution order:
    1. Session cookie (``request.session["user"]``).
    2. HTTP Basic credentials (hybrid mode only).
    3. Unauthenticated → 401 JSON or 303 redirect to ``/login``.
    """
    # --- 1. Session auth ---------------------------------------------------
    session_user = request.session.get("user")
    if session_user:
        return AuthenticatedUser(username=session_user)

    # --- 2. Basic auth fallback (hybrid mode) ------------------------------
    if settings.webapp_auth_mode == "hybrid" and credentials is not None:
        user = auth_provider.authenticate(
            {
                "username": credentials.username,
                "password": credentials.password,
            }
        )
        if user is not None:
            return user

    # --- 3. Unauthenticated ------------------------------------------------
    # Best-effort audit log
    try:
        from app.services.activity_service import get_activity_service

        svc = get_activity_service()
        svc.log_activity(
            user="anonymous",
            action="unauthorized_access",
            resource_type="auth",
            resource_id=request.url.path,
            details={"method": request.method, "path": str(request.url)},
            ip_address=_get_client_ip(request),
        )
    except Exception:
        logger.debug("Failed to log unauthorized_access event", exc_info=True)

    if wants_json(request):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    # HTML redirect to login page
    next_path = quote(request.url.path, safe="")
    raise _LoginRedirectException(next_path=next_path)
