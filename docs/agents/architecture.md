# Architecture (High Level)

This is a conceptual flow; consult service-level AGENTS for implementation detail.

```mermaid
graph TD
    subgraph "Docker Network"
        Daemon[Dagster Daemon]
        Sensor[Dagster Sensor]
        CodeLoc[User Code Container]

        Landing[(MinIO: Landing Zone)]
        Lake[(MinIO: Data Lake)]
        Mongo[(MongoDB: Ledger)]
        PostGIS[(PostGIS: Compute)]
    end

    User -->|1. Upload Files + Manifest| Landing
    Landing -->|2. Manifest detected| Sensor
    Sensor -->|3. Signal run| Daemon
    Daemon -->|4. Launch run| CodeLoc
    CodeLoc -->|5. Read raw data| Landing
    CodeLoc -->|6a. Spatial ops| PostGIS
    CodeLoc -->|6b. Tabular ops| CodeLoc
    CodeLoc -->|7. Archive raw source (artifact → blob)| Lake
    CodeLoc -->|8. Write Parquet/GeoParquet| Lake
    CodeLoc -->|9. Log lineage + audit| Mongo
```

## Data objects
- **Blobs**: content-addressed raw bytes stored under `s3://data-lake/blobs/...`.
- **Artifacts**: per-upload raw/intermediate references that point to blobs (includes source path + bucket).
- **Assets**: versioned, queryable outputs produced by the pipeline.
Raw source archival is content-addressed: uploads create **artifacts** (per upload instance) pointing to
deduplicated **blobs** in `s3://data-lake/blobs/...`. The archive flow is hash-first, upload-second so
existing blobs can be reused without re-reading entire files into memory.
