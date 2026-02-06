# =============================================================================
# Dagster Webserver/Daemon Dockerfile
# =============================================================================
# This image runs the Dagster webserver and daemon services.
# It does NOT include GDAL - that's in the user-code container.
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
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Install dependencies via uv
# ---------------------------------------------------------------------------
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /workspace
COPY pyproject.toml uv.lock .python-version ./
COPY libs/ libs/
RUN uv sync --frozen --no-install-project --only-group dagster

# ---------------------------------------------------------------------------
# Runtime environment
# ---------------------------------------------------------------------------
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create dagster home directory
RUN mkdir -p /opt/dagster/dagster_home

# Set environment
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Default command (overridden in docker-compose)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
