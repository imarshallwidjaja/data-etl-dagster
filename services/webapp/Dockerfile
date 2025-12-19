# =============================================================================
# Tooling Webapp Dockerfile
# =============================================================================
# FastAPI-based webapp for managing the data-etl-dagster pipeline.
# Build context should be repo root for access to libs/.
# =============================================================================

FROM python:3.11-slim

WORKDIR /opt/webapp

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY services/webapp/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install libs for shared model validation
COPY libs /opt/webapp/libs
RUN pip install --no-cache-dir -e /opt/webapp/libs

# Copy webapp source
COPY services/webapp/app /opt/webapp/app

# Environment defaults
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/webapp

EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
