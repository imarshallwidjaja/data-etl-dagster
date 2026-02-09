# =============================================================================
# Complex Spreadsheet Ops — Multi-table XLSX splitter
# =============================================================================
# Pure helpers for anchor detection, header composition, and melt;
# plus a Dagster op that splits a complex XLSX into per-sheet parquet files.
# =============================================================================

import tempfile
from pathlib import Path
from typing import Any, Optional

import openpyxl
import polars as pl
from openpyxl.worksheet.worksheet import Worksheet

from dagster import op, OpExecutionContext, In, Out

from libs.s3_utils import parse_s3_path
from .intermediate_artifacts import register_intermediate_from_local_file


# =============================================================================
# Pure helpers
# =============================================================================


def find_anchor_in_sheet(
    ws: Worksheet,
    anchor: str,
    mode: str = "exact",
) -> Optional[tuple[int, int]]:
    """
    Search an openpyxl worksheet for a cell matching *anchor*.

    Args:
        ws: openpyxl Worksheet (data already loaded).
        anchor: The string to search for.
        mode: ``"exact"`` for equality, ``"contains"`` for substring.

    Returns:
        ``(row_0idx, col_0idx)`` of the first match, or ``None``.
    """
    for row_idx, row in enumerate(ws.iter_rows(values_only=True)):
        for col_idx, cell_value in enumerate(row):
            if cell_value is None:
                continue
            cell_str = str(cell_value)
            if mode == "exact" and cell_str == anchor:
                return (row_idx, col_idx)
            if mode == "contains" and anchor in cell_str:
                return (row_idx, col_idx)
    return None


def compose_multi_row_header(rows: list[list]) -> list[str]:
    """
    Merge multiple header rows into a single list of column names.

    For each column index the non-empty values across all rows are joined
    with ``" "``.  Leading/trailing whitespace is stripped.

    Args:
        rows: A list of header rows (each row a list of cell values).

    Returns:
        A flat list of composed column name strings.
    """
    if not rows:
        return []
    ncols = max(len(r) for r in rows)

    composed: list[str] = []
    for col_idx in range(ncols):
        parts: list[str] = []
        for row in rows:
            val = row[col_idx] if col_idx < len(row) else None
            if val is not None:
                s = str(val).strip()
                if s:
                    parts.append(s)
        composed.append(" ".join(parts) if parts else "")
    return composed


def melt_to_long_format(
    df: pl.DataFrame,
    id_column_count: int,
) -> pl.DataFrame:
    """
    Unpivot (melt) a wide DataFrame to long format.

    The first *id_column_count* columns are treated as identifiers;
    the rest become ``variable`` / ``value`` pairs.

    Args:
        df: Wide-format Polars DataFrame.
        id_column_count: Number of leading columns to keep as ids.

    Returns:
        Long-format DataFrame with ``variable`` and ``value`` columns.
    """
    id_cols = df.columns[:id_column_count]
    value_cols = df.columns[id_column_count:]
    return df.unpivot(
        on=value_cols,
        index=id_cols,
    )


def process_workbook_sheets(
    *,
    xlsx_path: str,
    anchor: str,
    anchor_mode: str = "exact",
    header_rows: int = 1,
    id_column_count: int = 1,
) -> list[dict[str, Any]]:
    """
    Open a workbook, locate the anchor in each sheet, compose headers,
    slice the data region, trim trailing empties, and melt to long format.

    Args:
        xlsx_path: Path to the XLSX file on disk.
        anchor: Cell value that marks the top-left of the data region.
        anchor_mode: ``"exact"`` or ``"contains"``.
        header_rows: Number of rows that form the header (starting at anchor row).
        id_column_count: Number of leading columns treated as IDs for melt.

    Returns:
        List of dicts ``{"sheet_name": str, "dataframe": pl.DataFrame}``
        for each sheet where the anchor was found.

    Raises:
        ValueError: If no sheet contains the anchor.
    """
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    results: list[dict[str, Any]] = []

    try:
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            pos = find_anchor_in_sheet(ws, anchor=anchor, mode=anchor_mode)
            if pos is None:
                continue

            anchor_row, _anchor_col = pos

            # The anchor marks the *last* header row.
            # The header region starts (header_rows - 1) rows above the anchor.
            header_start = max(0, anchor_row - (header_rows - 1))

            # Read sheet via Polars (calamine engine) — raw, no header, keep empties
            raw_df = pl.read_excel(
                xlsx_path,
                sheet_name=sheet_name,
                engine="calamine",
                has_header=False,
                drop_empty_rows=False,
                drop_empty_cols=False,
                raise_if_empty=False,
            )

            if raw_df.is_empty():
                continue

            # Slice from header start onwards
            sliced = raw_df.slice(header_start)

            # --- Compose header ---
            header_raw_rows: list[list] = []
            for i in range(header_rows):
                if i < sliced.height:
                    header_raw_rows.append(sliced.row(i, named=False))
            headers = compose_multi_row_header(header_raw_rows)

            # Slice data below header rows
            data = sliced.slice(header_rows)

            # Rename columns to composed headers
            if len(headers) == data.width:
                data = data.rename(dict(zip(data.columns, headers)))
            else:
                # Trim or pad headers to match column count
                adjusted = headers[: data.width] + [
                    f"_col{i}" for i in range(len(headers), data.width)
                ]
                data = data.rename(dict(zip(data.columns, adjusted)))

            # Trim trailing all-null rows
            while data.height > 0:
                last_row = data.tail(1)
                if last_row.select(pl.all().is_null()).row(0) == tuple(
                    [True] * data.width
                ):
                    data = data.head(data.height - 1)
                else:
                    break

            if data.is_empty():
                continue

            # Melt to long format
            melted = melt_to_long_format(data, id_column_count=id_column_count)

            results.append({"sheet_name": sheet_name, "dataframe": melted})
    finally:
        wb.close()

    if not results:
        raise ValueError(
            f"No sheets contained the anchor '{anchor}' (mode={anchor_mode}). "
            "All sheets were skipped."
        )

    return results


# =============================================================================
# Dagster op
# =============================================================================


@op(
    ins={"manifest": In(dagster_type=dict)},
    out={"manifest": Out(dagster_type=dict)},
    required_resource_keys={"minio", "mongodb"},
)
def split_complex_spreadsheet_op(
    context: OpExecutionContext,
    manifest: dict,
) -> dict:
    """
    Split a complex multi-table XLSX into per-sheet parquet intermediates,
    register each as an artifact, and publish child manifests to the landing zone.

    Steps:
      1. Download the XLSX locally.
      2. Preflight collision check for all child manifest keys.
      3. For each matching sheet: read, anchor-slice, compose headers,
         melt, write parquet, register intermediate artifact, publish child manifest.

    Returns the parent manifest unchanged.
    """
    from libs.models import Manifest

    minio = context.resources.minio
    mongodb = context.resources.mongodb
    run_id = mongodb.get_run_object_id(context.run_id)

    # Parse and validate manifest
    validated = Manifest(**manifest)
    batch_id = validated.batch_id
    uploader = validated.uploader
    file_entry = validated.files[0]
    s3_path = file_entry.path

    cs_config = validated.metadata.complex_spreadsheet
    template_id = cs_config.template_id

    dataset_id_base = validated.metadata.tags.get("dataset_id", batch_id)

    # --- Step 1: Download XLSX ---
    bucket, s3_key = parse_s3_path(s3_path)
    tmp_xlsx = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    tmp_xlsx_path = tmp_xlsx.name
    tmp_xlsx.close()

    if bucket == minio.landing_bucket:
        context.log.info(f"Downloading XLSX from landing zone: {s3_key}")
        minio.download_from_landing(s3_key, tmp_xlsx_path)
    elif bucket == minio.lake_bucket:
        context.log.info(f"Downloading XLSX from data lake: {s3_key}")
        minio.download_from_lake(s3_key, tmp_xlsx_path)
    else:
        raise ValueError(f"Unsupported bucket '{bucket}' for XLSX download")

    # --- Determine template parameters ---
    if template_id != "anchor_unpivot_v1":
        raise ValueError(
            f"Unsupported template_id '{template_id}'. "
            "Only 'anchor_unpivot_v1' is currently supported."
        )

    params = cs_config.template_params
    anchor = params.anchor_text
    anchor_mode = params.anchor_match
    header_rows = params.header_rows
    id_column_count = params.id_column_count

    # --- Step 2: Process workbook ---
    try:
        sheet_results = process_workbook_sheets(
            xlsx_path=tmp_xlsx_path,
            anchor=anchor,
            anchor_mode=anchor_mode,
            header_rows=header_rows,
            id_column_count=id_column_count,
        )
    finally:
        Path(tmp_xlsx_path).unlink(missing_ok=True)

    # --- Step 3: Preflight collision check ---
    child_manifest_keys: list[str] = []
    for result in sheet_results:
        sheet_name = result["sheet_name"]
        safe_sheet = sheet_name.replace(" ", "_").lower()
        child_batch_id = f"{batch_id}__{safe_sheet}"
        child_key = f"manifests/{child_batch_id}.json"
        child_manifest_keys.append(child_key)

    collisions: list[str] = []
    for key in child_manifest_keys:
        if minio.object_exists_in_landing(key):
            collisions.append(key)
    if collisions:
        raise RuntimeError(
            f"Collision preflight failed — child manifests already exist: {collisions}. "
            "No side effects were produced."
        )

    # --- Step 4: Per-sheet processing ---
    for idx, result in enumerate(sheet_results):
        sheet_name = result["sheet_name"]
        df: pl.DataFrame = result["dataframe"]
        safe_sheet = sheet_name.replace(" ", "_").lower()
        child_batch_id = f"{batch_id}__{safe_sheet}"
        child_dataset_id = f"{dataset_id_base}__{safe_sheet}"

        context.log.info(
            f"Processing sheet '{sheet_name}' → child batch '{child_batch_id}' "
            f"({df.shape[0]} rows, {df.shape[1]} cols)"
        )

        # Write parquet to temp
        tmp_pq = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp_pq_path = tmp_pq.name
        tmp_pq.close()
        try:
            df.write_parquet(tmp_pq_path)

            # Register intermediate artifact
            artifact_info = register_intermediate_from_local_file(
                local_path=tmp_pq_path,
                batch_id=batch_id,
                run_id=run_id,
                producer=f"split_complex_spreadsheet_op::{template_id}",
                label=f"sheet:{sheet_name}",
                parameters={
                    "sheet_name": sheet_name,
                    "template_id": template_id,
                    "parent_batch_id": batch_id,
                },
                content_type="application/vnd.apache.parquet",
                minio=minio,
                mongodb=mongodb,
                log=context.log,
            )
        finally:
            Path(tmp_pq_path).unlink(missing_ok=True)

        # Build child manifest
        blob_s3_path = artifact_info["blob_s3_path"]
        child_manifest = {
            "batch_id": child_batch_id,
            "uploader": uploader,
            "intent": "ingest_tabular",
            "files": [
                {
                    "path": blob_s3_path,
                    "type": "tabular",
                    "format": "Parquet",
                }
            ],
            "metadata": {
                "title": f"{validated.metadata.title} — {sheet_name}",
                "description": validated.metadata.description,
                "keywords": validated.metadata.keywords,
                "source": validated.metadata.source,
                "license": validated.metadata.license,
                "attribution": validated.metadata.attribution,
                "project": validated.metadata.project,
                "tags": {
                    **validated.metadata.tags,
                    "dataset_id": child_dataset_id,
                    "parent_batch_id": batch_id,
                    "source_sheet": sheet_name,
                },
            },
        }

        child_key = child_manifest_keys[idx]
        context.log.info(f"Publishing child manifest: {child_key}")
        minio.upload_json_to_landing(child_key, child_manifest, if_not_exists=True)

    context.log.info(
        f"Split complete: {len(sheet_results)} child manifests published "
        f"from template '{template_id}'"
    )

    return manifest
