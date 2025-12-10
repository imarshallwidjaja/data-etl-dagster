# =============================================================================
# Dagster Webserver/Daemon Dockerfile
# =============================================================================
# This image runs the Dagster webserver and daemon services.
# It does NOT include GDAL - that's in the user-code container.
# =============================================================================

FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/dagster/app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create dagster home directory
RUN mkdir -p /opt/dagster/dagster_home

# Set environment
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Copy configuration files (will be mounted in docker-compose for dev)
# COPY dagster.yaml /opt/dagster/dagster_home/dagster.yaml
# COPY workspace.yaml /opt/dagster/app/workspace.yaml

# Default command (overridden in docker-compose)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]

