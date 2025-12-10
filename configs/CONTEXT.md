# Directory Context: Configs

## Overview

This directory contains configuration templates and shared configuration files for the Spatial ETL Pipeline.

## Purpose

- Store environment-specific configuration templates
- Provide base configurations that can be extended
- Centralize configuration management

## Contents (Planned)

```
configs/
├── CONTEXT.md           # This file
├── logging.yaml         # Logging configuration template
├── sensors.yaml         # Sensor configuration
└── resources.yaml       # Resource definitions
```

## Relation to Global Architecture

Configuration files in this directory are used by:
- Dagster user code for resource configuration
- Docker Compose for service configuration

## Notes

- Environment-specific values should use environment variables
- Secrets should NEVER be stored in this directory
- Use `.env` files for local development secrets

