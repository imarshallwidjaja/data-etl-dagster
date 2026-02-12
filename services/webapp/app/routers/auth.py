# =============================================================================
# Authentication Routes
# =============================================================================
# GET /login, POST /login, POST /logout — session issuance, rotation,
# safe redirect, CSRF enforcement, and audit logging.
# =============================================================================

import hmac
import logging
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, Form, Request

from app.security.csrf import require_csrf
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.responses import Response
from fastapi.templating import Jinja2Templates
from starlette import status

from app.auth.providers import BasicAuthProvider
from app.auth.session import new_csrf_token, set_session_user
from app.config import Settings, get_settings

logger = logging.getLogger(__name__)

router = APIRouter(tags=["auth"])

# Reuse the shared templates directory.
from pathlib import Path

_TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


def _get_auth_provider(settings: Settings = Depends(get_settings)) -> BasicAuthProvider:
    """Build a BasicAuthProvider from current settings."""
    return BasicAuthProvider(
        username=settings.webapp_username,
        password=settings.webapp_password,
    )


def _get_client_ip(request: Request) -> Optional[str]:
    """Extract client IP from X-Forwarded-For or request.client."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return None


def _is_safe_next(next_url: str) -> bool:
    """Return True only if *next_url* is a relative path (no scheme/netloc)."""
    if not next_url:
        return False

    # Backslashes are unsafe: user agents may normalize them to forward slashes
    # in Location handling (e.g. "\\/evil.com" → "//evil.com").
    if "\\" in next_url:
        return False

    parsed = urlparse(next_url)
    # Block absolute URLs: any scheme or netloc → unsafe.
    # Also block protocol-relative URLs (//evil.com).
    if parsed.scheme or parsed.netloc:
        return False
    if next_url.startswith("//"):
        return False
    return True


# ---------------------------------------------------------------------------
# GET /login
# ---------------------------------------------------------------------------


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    """Render the login form, seeding the session with a CSRF token."""
    # Ensure the anonymous session has a CSRF token.
    if "csrf" not in request.session:
        request.session["csrf"] = new_csrf_token()

    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "csrf_token": request.session["csrf"],
            "error": None,
            "next": request.query_params.get("next", ""),
        },
    )


# ---------------------------------------------------------------------------
# POST /login
# ---------------------------------------------------------------------------


@router.post("/login", response_model=None)
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(None),
    auth_provider: BasicAuthProvider = Depends(_get_auth_provider),
) -> Response:
    """
    Validate credentials and issue a session.

    CSRF is enforced: the submitted ``csrf_token`` must match the one in the
    session.  On failure the form is re-rendered (200) with a generic error.
    On success a 303 redirect is issued.
    """
    next_url = request.query_params.get("next", "")

    # --- CSRF check (timing-safe) -------------------------------------------
    session_csrf = request.session.get("csrf", "")
    if not csrf_token or not hmac.compare_digest(csrf_token, session_csrf):
        return HTMLResponse(
            content="CSRF validation failed",
            status_code=status.HTTP_403_FORBIDDEN,
        )

    # --- Credential check ---------------------------------------------------
    user = auth_provider.authenticate({"username": username, "password": password})

    ip = _get_client_ip(request)

    if user is None:
        # Audit: login_failure
        _log_activity(
            action="login_failure",
            user=username,
            ip_address=ip,
            details={"reason": "invalid_credentials"},
        )

        # Re-render with fresh CSRF (the old one is still valid in session).
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "csrf_token": request.session.get("csrf", ""),
                "error": "Invalid username or password",
                "next": next_url,
            },
            status_code=200,
        )

    # --- Success: rotate session (fixation protection) ----------------------
    set_session_user(request.session, user.username)

    # Audit: login_success
    _log_activity(
        action="login_success",
        user=user.username,
        ip_address=ip,
        details={},
    )

    redirect_to = next_url if _is_safe_next(next_url) else "/"
    return RedirectResponse(
        url=redirect_to,
        status_code=status.HTTP_303_SEE_OTHER,
    )


# ---------------------------------------------------------------------------
# POST /logout
# ---------------------------------------------------------------------------


@router.post("/logout", dependencies=[Depends(require_csrf)])
async def logout(request: Request) -> RedirectResponse:
    """Clear the session and redirect to the login page."""
    username = request.session.get("user", "anonymous")
    ip = _get_client_ip(request)

    request.session.clear()

    # Audit: logout
    _log_activity(
        action="logout",
        user=username,
        ip_address=ip,
        details={},
    )

    return RedirectResponse(
        url="/login",
        status_code=status.HTTP_303_SEE_OTHER,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _log_activity(
    *,
    action: str,
    user: str,
    ip_address: Optional[str] = None,
    details: Optional[dict] = None,
) -> None:
    """Best-effort audit log; never raises."""
    try:
        from app.services.activity_service import get_activity_service

        svc = get_activity_service()
        svc.log_activity(
            user=user,
            action=action,
            resource_type="auth",
            resource_id="session",
            details=details or {},
            ip_address=ip_address,
        )
    except Exception:
        logger.debug("Failed to log %s event", action, exc_info=True)
