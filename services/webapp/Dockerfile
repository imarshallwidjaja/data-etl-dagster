# =============================================================================
# Tooling Webapp Dockerfile
# =============================================================================
# FastAPI-based webapp for managing the data-etl-dagster pipeline.
# Build context should be repo root for access to libs/.
# =============================================================================

# ---------------------------------------------------------------------------
# uv — pinned to the project-wide version (.uv-version)
# ---------------------------------------------------------------------------
ARG UV_VERSION=0.9.22
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:3.10-slim

COPY --from=uv /uv /uvx /bin/

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Install dependencies via uv
# ---------------------------------------------------------------------------
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /workspace
COPY pyproject.toml uv.lock .python-version ./
COPY libs/ libs/
RUN uv sync --frozen --no-install-project --only-group webapp

# ---------------------------------------------------------------------------
# Runtime environment
# ---------------------------------------------------------------------------
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /opt/webapp

# Copy webapp source
COPY services/webapp/app /opt/webapp/app

# Environment defaults
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/webapp:/workspace

EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
