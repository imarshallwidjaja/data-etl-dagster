# =============================================================================
# FastAPI Main Application
# =============================================================================
# Entry point for the tooling webapp.
# =============================================================================

from pathlib import Path

from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.auth.dependencies import AuthenticatedUser, get_current_user
from app.routers import health, landing, manifests, runs, assets

# Application instance
app = FastAPI(
    title="Data ETL Tooling Webapp",
    description="Manage the data-etl-dagster pipeline without direct access to Dagster, MongoDB, or MinIO.",
    version="0.1.0",
)

# Paths
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Mount static files if directory exists
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Include routers
app.include_router(health.router)
app.include_router(landing.router)
app.include_router(manifests.router)
app.include_router(runs.router)
app.include_router(assets.router)


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> HTMLResponse:
    """
    Landing page - requires authentication.

    Shows navigation to main sections:
    - Landing Zone Management
    - Manifest Creation
    - Run Tracking
    - Asset Browsing
    """
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "user": current_user,
        },
    )
