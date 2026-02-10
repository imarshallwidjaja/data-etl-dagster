"""Unit tests for complex spreadsheet splitter helpers."""

from __future__ import annotations

import polars as pl
import pytest
from openpyxl import Workbook

from services.dagster.etl_pipelines.ops.complex_spreadsheet_ops import (
    _compose_multirow_headers,
    _find_anchor_row,
    _melt_with_id_columns,
    _prepare_sheet_specs,
)


def _build_workbook_with_sheets() -> Workbook:
    wb = Workbook()
    ws = wb.active
    ws.title = "AnchorSheet"
    ws.append(["noise", "noise"])
    ws.append(["Region", "Income"])
    ws.append(["", "2020"])
    ws.append(["NSW", 10])
    ws.append(["VIC", 12])

    ws2 = wb.create_sheet("NoAnchor")
    ws2.append(["no", "match"])

    return wb


def test_find_anchor_row_supports_exact_contains_and_regex_modes():
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws.append(["Before"])
    ws.append(["Region anchor"])

    assert _find_anchor_row(ws, anchor_text="Region anchor", anchor_match="exact") == 1
    assert _find_anchor_row(ws, anchor_text="anchor", anchor_match="contains") == 1
    assert (
        _find_anchor_row(ws, anchor_text=r"Region\s+anchor", anchor_match="regex") == 1
    )


def test_compose_multirow_headers_combines_rows_and_normalizes_names():
    header_block = pl.DataFrame(
        [
            ["Region", "Median Income", "Median Income"],
            ["", "2019", "2020"],
        ],
        schema=["column_1", "column_2", "column_3"],
        orient="row",
    )

    headers = _compose_multirow_headers(header_block)

    assert headers == ["region", "median_income_2019", "median_income_2020"]


def test_melt_with_id_column_count_unpivots_wide_table_to_long_shape():
    table = pl.DataFrame(
        {
            "region": ["NSW", "VIC"],
            "median_income_2019": [10, 20],
            "median_income_2020": [11, 22],
        }
    )

    melted = _melt_with_id_columns(table, id_column_count=1)

    assert melted.columns == ["region", "variable", "value"]
    assert melted.height == 4
    assert set(melted["variable"].to_list()) == {
        "median_income_2019",
        "median_income_2020",
    }


def test_prepare_sheet_specs_skips_sheet_missing_anchor():
    workbook = _build_workbook_with_sheets()

    specs, skipped_sheets = _prepare_sheet_specs(
        workbook=workbook,
        anchor_text="Region",
        anchor_match="exact",
        sheet_names=None,
    )

    assert [spec.sheet_name for spec in specs] == ["AnchorSheet"]
    assert skipped_sheets == ["NoAnchor"]


def test_prepare_sheet_specs_errors_when_all_sheets_are_skipped():
    workbook = Workbook()
    ws = workbook.active
    ws.title = "OnlySheet"
    ws.append(["x", "y"])

    with pytest.raises(ValueError, match="No sheets matched anchor"):
        _prepare_sheet_specs(
            workbook=workbook,
            anchor_text="Region",
            anchor_match="exact",
            sheet_names=None,
        )
