# =============================================================================
# Test Runner Container
# =============================================================================
# Lightweight Python 3.10 image with pinned uv for running pytest inside the
# Docker network.  No GDAL/heavy spatial libs — unit + integration tests only.
#
# Build context: repo root (so we can COPY pyproject.toml, uv.lock, etc.)
# =============================================================================

# ---------------------------------------------------------------------------
# uv — pinned to the project-wide version (.uv-version)
# ---------------------------------------------------------------------------
ARG UV_VERSION=0.9.22
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:3.10-slim

COPY --from=uv /uv /uvx /bin/

# ---------------------------------------------------------------------------
# Non-root user
# ---------------------------------------------------------------------------
RUN groupadd --gid 1000 testrunner \
    && useradd --uid 1000 --gid testrunner --create-home testrunner

WORKDIR /app

# ---------------------------------------------------------------------------
# Dependencies — install before copying source for layer caching
# ---------------------------------------------------------------------------
COPY pyproject.toml uv.lock .python-version ./
COPY libs/ libs/
RUN uv sync --frozen --group test
ENV PATH="/app/.venv/bin:${PATH}"

# ---------------------------------------------------------------------------
# Source code — pytest.ini sets pythonpath to ". services/webapp" so tests
# import from services.dagster.*, scripts.*, etc. via the repo root.
# ---------------------------------------------------------------------------
COPY pytest.ini ./
COPY tests/ tests/
COPY services/dagster/etl_pipelines/ services/dagster/etl_pipelines/
COPY services/webapp/ services/webapp/
COPY services/mongodb/migrations/ services/mongodb/migrations/
COPY scripts/ scripts/
COPY configs/ configs/

# ---------------------------------------------------------------------------
# Own everything and drop to non-root
# ---------------------------------------------------------------------------
RUN chown -R testrunner:testrunner /app
USER testrunner

# Default: run unit tests; callers override via compose command / docker run.
CMD ["uv", "run", "pytest", "tests/unit", "-q"]
