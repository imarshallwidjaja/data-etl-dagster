# =============================================================================
# Authentication Dependencies
# =============================================================================
# FastAPI dependencies for authentication.
# =============================================================================

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.auth.providers import AuthenticatedUser, BasicAuthProvider
from app.config import Settings, get_settings

security = HTTPBasic()


def get_auth_provider(settings: Settings = Depends(get_settings)) -> BasicAuthProvider:
    """Get the authentication provider instance."""
    return BasicAuthProvider(
        username=settings.webapp_username,
        password=settings.webapp_password,
    )


def get_current_user(
    credentials: HTTPBasicCredentials = Depends(security),
    auth_provider: BasicAuthProvider = Depends(get_auth_provider),
) -> AuthenticatedUser:
    """
    Validate HTTP Basic Auth credentials and return the authenticated user.

    Raises:
        HTTPException: 401 Unauthorized if credentials are invalid.
    """
    user = auth_provider.authenticate(
        {
            "username": credentials.username,
            "password": credentials.password,
        }
    )

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )

    return user
