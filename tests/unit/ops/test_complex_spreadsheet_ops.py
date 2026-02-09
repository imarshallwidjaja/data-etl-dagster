# =============================================================================
# Unit Tests: Complex Spreadsheet Ops (pure helpers)
# =============================================================================

import pytest
import openpyxl
import tempfile
from pathlib import Path
from unittest.mock import Mock

from services.dagster.etl_pipelines.ops.complex_spreadsheet_ops import (
    find_anchor_in_sheet,
    compose_multi_row_header,
    melt_to_long_format,
    process_workbook_sheets,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_xlsx(sheets: dict[str, list[list]]) -> str:
    """Create a temp XLSX from {sheet_name: [[row1], [row2], ...]}."""
    wb = openpyxl.Workbook()
    # Remove default sheet
    wb.remove(wb.active)
    for name, rows in sheets.items():
        ws = wb.create_sheet(title=name)
        for row in rows:
            ws.append(row)
    path = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False).name
    wb.save(path)
    return path


# =============================================================================
# Test: find_anchor_in_sheet — exact mode
# =============================================================================


class TestFindAnchorExact:
    def test_exact_match_returns_row_col(self):
        """Anchor 'Year' found at exact cell returns (row, col) 0-indexed."""
        wb = openpyxl.Workbook()
        ws = wb.active
        # Row 1-3 are junk, anchor 'Year' at row 4 col 2 (B4 in 1-index)
        ws.append(["Title line"])
        ws.append(["Source: blah"])
        ws.append(["", ""])
        ws.append(["", "Year", "Value"])
        result = find_anchor_in_sheet(ws, anchor="Year", mode="exact")
        # 0-indexed: row=3, col=1
        assert result == (3, 1)

    def test_exact_match_not_found_returns_none(self):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["A", "B", "C"])
        result = find_anchor_in_sheet(ws, anchor="Year", mode="exact")
        assert result is None

    def test_exact_match_is_case_sensitive(self):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["year"])
        result = find_anchor_in_sheet(ws, anchor="Year", mode="exact")
        assert result is None


# =============================================================================
# Test: find_anchor_in_sheet — contains mode
# =============================================================================


class TestFindAnchorContains:
    def test_contains_match(self):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Some preamble"])
        ws.append(["", "Region Name", "Count"])
        result = find_anchor_in_sheet(ws, anchor="Region", mode="contains")
        assert result == (1, 1)

    def test_contains_no_match(self):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["A", "B"])
        result = find_anchor_in_sheet(ws, anchor="Region", mode="contains")
        assert result is None


# =============================================================================
# Test: compose_multi_row_header
# =============================================================================


class TestComposeMultiRowHeader:
    def test_single_header_row(self):
        """Single header row returns values as-is (stripped)."""
        rows = [["Region", "2020", "2021"]]
        result = compose_multi_row_header(rows)
        assert result == ["Region", "2020", "2021"]

    def test_two_header_rows_merged(self):
        """Two header rows are concatenated with ' ' separator."""
        rows = [
            ["", "Population", "Population"],
            ["Region", "Male", "Female"],
        ]
        result = compose_multi_row_header(rows)
        assert result == ["Region", "Population Male", "Population Female"]

    def test_none_and_empty_cells_skipped(self):
        """None/empty cells in upper row are ignored; only lower value used."""
        rows = [
            [None, "Group A", None],
            ["ID", "Val1", "Val2"],
        ]
        result = compose_multi_row_header(rows)
        assert result == ["ID", "Group A Val1", "Val2"]

    def test_whitespace_trimmed(self):
        rows = [["  Region  ", " Value "]]
        result = compose_multi_row_header(rows)
        assert result == ["Region", "Value"]


# =============================================================================
# Test: melt_to_long_format
# =============================================================================


class TestMeltToLongFormat:
    def test_basic_melt(self):
        """id_column_count=1: first col is id, rest melted to variable+value."""
        import polars as pl

        df = pl.DataFrame(
            {
                "Region": ["NSW", "VIC"],
                "2020": [100, 200],
                "2021": [110, 210],
            }
        )
        result = melt_to_long_format(df, id_column_count=1)
        assert result.shape == (4, 3)
        assert set(result.columns) == {"Region", "variable", "value"}
        # Check values
        nsw_2020 = result.filter(
            (pl.col("Region") == "NSW") & (pl.col("variable") == "2020")
        )
        assert nsw_2020["value"].to_list() == [100]

    def test_two_id_columns(self):
        """id_column_count=2: first two cols are ids."""
        import polars as pl

        df = pl.DataFrame(
            {
                "State": ["NSW", "VIC"],
                "Region": ["Syd", "Melb"],
                "2020": [100, 200],
            }
        )
        result = melt_to_long_format(df, id_column_count=2)
        assert result.shape == (2, 4)
        assert set(result.columns) == {"State", "Region", "variable", "value"}

    def test_zero_id_columns(self):
        """id_column_count=0: all cols melted."""
        import polars as pl

        df = pl.DataFrame({"A": [1], "B": [2]})
        result = melt_to_long_format(df, id_column_count=0)
        assert result.shape == (2, 2)
        assert set(result.columns) == {"variable", "value"}


# =============================================================================
# Test: process_workbook_sheets — sheet missing anchor is skipped
# =============================================================================


class TestProcessWorkbookSheets:
    def test_sheet_missing_anchor_skipped(self):
        """Sheets where anchor is not found are skipped, no error."""
        path = _make_xlsx(
            {
                "Data1": [
                    ["Preamble"],
                    ["Year", "Value"],
                    [2020, 100],
                ],
                "Notes": [
                    ["This sheet has no anchor"],
                    ["Just text"],
                ],
            }
        )
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor="Year",
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
        )
        # Only Data1 should produce a result
        assert len(results) == 1
        assert results[0]["sheet_name"] == "Data1"

    def test_all_sheets_skipped_raises(self):
        """If all sheets are skipped (no anchor found), raise ValueError."""
        path = _make_xlsx(
            {
                "Notes1": [["No anchor here"]],
                "Notes2": [["Still nothing"]],
            }
        )
        with pytest.raises(ValueError, match="No sheets.*anchor"):
            process_workbook_sheets(
                xlsx_path=path,
                anchor="Year",
                anchor_mode="exact",
                header_rows=1,
                id_column_count=1,
            )

    def test_multi_row_header_and_melt(self):
        """Integration: 2-row header composed, data melted correctly."""
        path = _make_xlsx(
            {
                "Data": [
                    ["Title line — ignore"],
                    ["", "Population", "Population"],
                    ["Region", "Male", "Female"],
                    ["NSW", 100, 200],
                    ["VIC", 300, 400],
                ],
            }
        )
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor="Region",
            anchor_mode="contains",
            header_rows=2,
            id_column_count=1,
        )
        assert len(results) == 1
        df = results[0]["dataframe"]
        assert set(df.columns) == {"Region", "variable", "value"}
        assert df.shape[0] == 4  # 2 regions × 2 value columns

    def test_trailing_empty_rows_trimmed(self):
        """Trailing all-null rows are removed from the data."""
        path = _make_xlsx(
            {
                "Data": [
                    ["Year", "Value"],
                    [2020, 100],
                    [2021, 200],
                    [None, None],
                    [None, None],
                ],
            }
        )
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor="Year",
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
        )
        df = results[0]["dataframe"]
        # 2 data rows × 1 value column = 2 rows after melt
        assert df.shape[0] == 2
