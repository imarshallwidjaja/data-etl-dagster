# =============================================================================
# Unit Tests: Complex Spreadsheet Ops (pure helpers)
# =============================================================================

import shutil

import pytest
import openpyxl
from pathlib import Path
from unittest.mock import Mock, patch

from pydantic import ValidationError

from services.dagster.etl_pipelines.ops.complex_spreadsheet_ops import (
    find_anchor_in_sheet,
    compose_multi_row_header,
    melt_to_long_format,
    process_workbook_sheets,
    process_workbook_sheets_v2,
    slugify_sheet_name_v2,
    split_complex_spreadsheet_op,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture()
def make_xlsx(tmp_path: Path):
    """Factory fixture: create XLSX files in the pytest temp dir.

    Usage::

        path = make_xlsx({"Sheet1": [["A", "B"], [1, 2]]})
    """

    _counter = 0

    def _factory(sheets: dict[str, list[list]]) -> str:
        nonlocal _counter
        _counter += 1
        wb = openpyxl.Workbook()
        wb.remove(wb.active)
        for name, rows in sheets.items():
            ws = wb.create_sheet(title=name)
            for row in rows:
                ws.append(row)
        path = tmp_path / f"workbook_{_counter}.xlsx"
        wb.save(str(path))
        return str(path)

    return _factory


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


class TestFindAnchorV2:
    def test_exact_match_is_case_insensitive(self):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["year"])

        from services.dagster.etl_pipelines.ops.complex_spreadsheet_ops import (
            find_anchor_in_sheet_v2,
        )

        result = find_anchor_in_sheet_v2(ws, anchor="Year", mode="exact")
        assert result == (0, 0)

    def test_contains_match_is_case_insensitive(self):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["", "REGION NAME", "Count"])

        from services.dagster.etl_pipelines.ops.complex_spreadsheet_ops import (
            find_anchor_in_sheet_v2,
        )

        result = find_anchor_in_sheet_v2(ws, anchor="region", mode="contains")
        assert result == (0, 1)

    def test_regex_match_is_case_insensitive(self):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["", "Region Name", "Count"])

        from services.dagster.etl_pipelines.ops.complex_spreadsheet_ops import (
            find_anchor_in_sheet_v2,
        )

        result = find_anchor_in_sheet_v2(
            ws,
            anchor=r"^region\s+name$",
            mode="regex",
        )
        assert result == (0, 1)

    def test_regex_no_match_returns_none(self):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["", "Region Name", "Count"])

        from services.dagster.etl_pipelines.ops.complex_spreadsheet_ops import (
            find_anchor_in_sheet_v2,
        )

        result = find_anchor_in_sheet_v2(ws, anchor=r"^year$", mode="regex")
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
    def test_sheet_missing_anchor_skipped(self, make_xlsx):
        """Sheets where anchor is not found are skipped, no error."""
        path = make_xlsx(
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

    def test_all_sheets_skipped_raises(self, make_xlsx):
        """If all sheets are skipped (no anchor found), raise ValueError."""
        path = make_xlsx(
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

    def test_multi_row_header_and_melt(self, make_xlsx):
        """Integration: 2-row header composed, data melted correctly."""
        path = make_xlsx(
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

    def test_trailing_empty_rows_trimmed(self, make_xlsx):
        """Trailing all-null rows are removed from the data."""
        path = make_xlsx(
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

    def test_sheet_names_filter_processes_only_listed_sheets(self, make_xlsx):
        """When sheet_names is provided, only those sheets are processed."""
        path = make_xlsx(
            {
                "Data1": [
                    ["Year", "Value"],
                    [2020, 100],
                ],
                "Data2": [
                    ["Year", "Value"],
                    [2021, 200],
                ],
                "Notes": [
                    ["Year", "Value"],
                    [2022, 300],
                ],
            }
        )
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor="Year",
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
            sheet_names=["Data1", "Data2"],
        )
        sheet_names_result = [r["sheet_name"] for r in results]
        assert "Data1" in sheet_names_result
        assert "Data2" in sheet_names_result
        assert "Notes" not in sheet_names_result
        assert len(results) == 2

    def test_sheet_names_none_processes_all_sheets(self, make_xlsx):
        """When sheet_names is None, all sheets with anchors are processed."""
        path = make_xlsx(
            {
                "Data1": [
                    ["Year", "Value"],
                    [2020, 100],
                ],
                "Data2": [
                    ["Year", "Value"],
                    [2021, 200],
                ],
                "Notes": [
                    ["Year", "Value"],
                    [2022, 300],
                ],
            }
        )
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor="Year",
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
            sheet_names=None,
        )
        sheet_names_result = [r["sheet_name"] for r in results]
        assert len(results) == 3
        assert "Data1" in sheet_names_result
        assert "Data2" in sheet_names_result
        assert "Notes" in sheet_names_result

    def test_sheet_names_empty_list_processes_all(self, make_xlsx):
        """When sheet_names=[], behaves like None and processes all sheets."""
        path = make_xlsx(
            {
                "Data1": [
                    ["Year", "Value"],
                    [2020, 100],
                ],
            }
        )
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor="Year",
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
            sheet_names=[],
        )
        # Empty list normalised to None → process all sheets
        assert len(results) == 1
        assert results[0]["sheet_name"] == "Data1"

    def test_sheet_names_nonexistent_sheet_ignored(self, make_xlsx):
        """Non-existent sheet names in filter are silently ignored."""
        path = make_xlsx(
            {
                "Data1": [
                    ["Year", "Value"],
                    [2020, 100],
                ],
                "Data2": [
                    ["Year", "Value"],
                    [2021, 200],
                ],
            }
        )
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor="Year",
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
            sheet_names=["Data1", "NonExistent"],
        )
        assert len(results) == 1
        assert results[0]["sheet_name"] == "Data1"

    def test_anchor_column_slices_leading_junk_columns(self, make_xlsx):
        """When anchor is not in column 0, columns before anchor are dropped."""
        # Layout: col0 is junk, col1 is junk, anchor 'Year' at col2
        # Row 0: ["junk", "junk", "Year", "Value"]
        # Row 1: ["x",    "y",    2020,   100   ]
        # Row 2: ["x",    "y",    2021,   200   ]
        path = make_xlsx(
            {
                "Data": [
                    ["junk0", "junk1", "Year", "Value"],
                    ["x", "y", 2020, 100],
                    ["x", "y", 2021, 200],
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
        assert len(results) == 1
        df = results[0]["dataframe"]
        # Should only have 'Year' and 'Value' columns (melted to long format)
        # junk0 and junk1 should be stripped
        assert "variable" in df.columns
        assert "value" in df.columns
        # The id column should be 'Year', not 'junk0'
        assert "Year" in df.columns
        assert "junk0" not in df.columns
        assert "junk1" not in df.columns

    def test_anchor_at_col0_no_column_slice(self, make_xlsx):
        """When anchor is in column 0, no columns are dropped (regression guard)."""
        path = make_xlsx(
            {
                "Data": [
                    ["Year", "Value"],
                    [2020, 100],
                    [2021, 200],
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
        assert len(results) == 1
        df = results[0]["dataframe"]
        assert "Year" in df.columns
        assert df.shape[0] == 2

    def test_anchor_none_defaults_to_top_left(self, make_xlsx):
        """When anchor is None, processing starts at the top-left cell."""
        path = make_xlsx({"Data": [["Year", "Value"], [2020, 100]]})
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor=None,
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
        )
        assert len(results) == 1

    def test_anchor_whitespace_defaults_to_top_left(self, make_xlsx):
        """When anchor is blank/whitespace, processing starts at top-left cell."""
        path = make_xlsx({"Data": [["Year", "Value"], [2020, 100]]})
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor="   ",
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
        )
        assert len(results) == 1


# =============================================================================
# Test: process_workbook_sheets_v2
# =============================================================================


class TestProcessWorkbookSheetsV2:
    def test_case_insensitive_exact_anchor_match(self, make_xlsx):
        """v2 exact matching is case-insensitive."""
        path = make_xlsx(
            {
                "Data": [
                    ["Year", "Value"],
                    [2020, 100],
                ],
            }
        )

        results = process_workbook_sheets_v2(
            xlsx_path=path,
            anchor="year",
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
        )

        assert len(results) == 1
        df = results[0]["dataframe"]
        assert set(df.columns) == {"year", "variable", "value"}
        assert df.shape[0] == 1

    def test_regex_anchor_match_and_header_normalization(self, make_xlsx):
        """v2 supports regex anchors and normalizes composed headers."""
        path = make_xlsx(
            {
                "Data": [
                    ["Preamble"],
                    ["", "Population (%)", "Population (%)"],
                    ["Region Name", "Male", "Female"],
                    ["NSW", 100, 200],
                    ["VIC", 300, 400],
                ],
            }
        )

        results = process_workbook_sheets_v2(
            xlsx_path=path,
            anchor=r"region\s+name",
            anchor_mode="regex",
            header_rows=2,
            id_column_count=1,
        )

        assert len(results) == 1
        df = results[0]["dataframe"]
        assert set(df.columns) == {"region_name", "variable", "value"}
        assert df.shape[0] == 4
        assert sorted(df["variable"].unique().to_list()) == sorted(
            ["population_male", "population_female"]
        )

    def test_regex_with_blank_anchor_defaults_to_top_left(self, make_xlsx):
        """v2 regex mode with blank anchor_text falls back to top-left anchor."""
        path = make_xlsx(
            {
                "Data": [
                    ["Region", "2020"],
                    ["NSW", 100],
                ],
            }
        )

        results = process_workbook_sheets_v2(
            xlsx_path=path,
            anchor="   ",
            anchor_mode="regex",
            header_rows=1,
            id_column_count=1,
        )

        assert len(results) == 1
        df = results[0]["dataframe"]
        assert set(df.columns) == {"region", "variable", "value"}
        assert df.shape[0] == 1


# =============================================================================
# Test: slugify_sheet_name_v2
# =============================================================================


class TestSlugifyV2:
    def test_slugify_sheet_name_v2_normalizes_case_and_symbols(self):
        assert slugify_sheet_name_v2("  Population (%) 2024 ") == "population_2024"

    def test_slugify_sheet_name_v2_collapses_whitespace_and_underscores(self):
        assert slugify_sheet_name_v2("Data__   Sheet") == "data_sheet"

    def test_slugify_sheet_name_v2_falls_back_to_sheet(self):
        assert slugify_sheet_name_v2("!!!") == "sheet"


# =============================================================================
# Test: slug uniqueness in split_complex_spreadsheet_op
# =============================================================================


class TestSlugUniqueness:
    def test_duplicate_slug_raises_valueerror(self):
        """Sheet names that normalize to the same slug must raise ValueError."""
        from services.dagster.etl_pipelines.ops.complex_spreadsheet_ops import (
            _check_slug_uniqueness_with,
            slugify_sheet_name_v2,
        )

        # "Data 1" and "Data_1" both normalize to "data_1"
        sheet_results = [
            {"sheet_name": "Data 1", "dataframe": None},
            {"sheet_name": "Data_1", "dataframe": None},
        ]
        with pytest.raises(ValueError, match="Duplicate child key slug"):
            _check_slug_uniqueness_with(sheet_results, slugify_sheet_name_v2)

    def test_unique_slugs_pass(self):
        """Distinct slugs should not raise."""
        from services.dagster.etl_pipelines.ops.complex_spreadsheet_ops import (
            _check_slug_uniqueness_with,
            slugify_sheet_name_v2,
        )

        sheet_results = [
            {"sheet_name": "Data 1", "dataframe": None},
            {"sheet_name": "Data 2", "dataframe": None},
        ]
        # Should not raise
        _check_slug_uniqueness_with(sheet_results, slugify_sheet_name_v2)

    def test_case_collision_raises(self):
        """Sheet names differing only in case normalize to same slug."""
        from services.dagster.etl_pipelines.ops.complex_spreadsheet_ops import (
            _check_slug_uniqueness_with,
            slugify_sheet_name_v2,
        )

        sheet_results = [
            {"sheet_name": "DATA 1", "dataframe": None},
            {"sheet_name": "Data 1", "dataframe": None},
        ]
        with pytest.raises(ValueError, match="Duplicate child key slug"):
            _check_slug_uniqueness_with(sheet_results, slugify_sheet_name_v2)


# =============================================================================
# Test: Non-first-column anchor — column count correctness
# =============================================================================


class TestNonFirstColumnAnchor:
    def test_anchor_at_col2_produces_correct_column_count(self, make_xlsx):
        """Anchor at column 2 strips 2 leading junk columns; output has correct width."""
        # 4 raw columns, anchor at col 2 → 2 usable columns (Year, Value)
        # After melt with id_column_count=1: 3 columns (Year, variable, value)
        path = make_xlsx(
            {
                "Data": [
                    ["junk_a", "junk_b", "Year", "2020", "2021"],
                    ["x", "y", "NSW", 100, 110],
                    ["x", "y", "VIC", 200, 210],
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
        assert len(results) == 1
        df = results[0]["dataframe"]
        # After slicing junk: 3 usable columns (Year, 2020, 2021)
        # After melt with id_column_count=1: Year is id, 2020/2021 become variable/value
        assert set(df.columns) == {"Year", "variable", "value"}
        # 2 regions × 2 value columns = 4 rows
        assert df.shape[0] == 4
        # Verify no junk columns leaked through
        all_vars = df["variable"].unique().to_list()
        assert "junk_a" not in all_vars
        assert "junk_b" not in all_vars


# =============================================================================
# Test: Unknown template_id raises ValueError
# =============================================================================


class TestUnknownTemplateId:
    def test_unknown_template_id_raises(self):
        """split_complex_spreadsheet_op raises for unknown template_id.

        The Literal constraint on ComplexSpreadsheetConfig.template_id means
        Pydantic validation rejects bad values before the runtime guard fires.
        Either way, the op must not proceed with an unsupported template.
        """
        from dagster import build_op_context

        manifest = {
            "batch_id": "batch_001",
            "uploader": "user_123",
            "intent": "ingest_complex_spreadsheet",
            "files": [
                {
                    "path": "s3://landing-zone/batch_001/data.xlsx",
                    "type": "tabular",
                    "format": "XLSX",
                }
            ],
            "metadata": {
                "title": "Test",
                "description": "Test",
                "keywords": ["test"],
                "source": "Unit Test",
                "license": "MIT",
                "attribution": "Test",
                "project": "ALPHA",
                "tags": {},
                "complex_spreadsheet": {
                    "template_id": "nonexistent_template_v99",
                    "template_params": {"anchor_text": "Year"},
                },
            },
        }

        mock_minio = Mock()
        mock_minio.landing_bucket = "landing-zone"
        mock_minio.lake_bucket = "data-lake"

        mock_mongodb = Mock()
        mock_mongodb.get_run_object_id.return_value = "60a1f77bcf86cd799439022"

        context = build_op_context(
            resources={"minio": mock_minio, "mongodb": mock_mongodb},
        )

        # The Manifest model's Literal["anchor_unpivot_v1"] constraint
        # causes a ValidationError before the runtime guard is reached.
        with pytest.raises(ValidationError, match="template_id"):
            split_complex_spreadsheet_op(context, manifest)


# =============================================================================
# Test: Sheet with only headers (no data rows) is skipped
# =============================================================================


class TestHeaderOnlySheet:
    def test_sheet_with_header_only_skipped(self, make_xlsx):
        """Sheet with anchor and header row but no data rows is skipped."""
        path = make_xlsx(
            {
                "HeaderOnly": [
                    ["Preamble"],
                    ["Year", "Value"],
                    # No data rows below the header
                ],
                "WithData": [
                    ["Year", "Value"],
                    [2020, 100],
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
        # HeaderOnly should be skipped (data.is_empty() after slicing header)
        assert len(results) == 1
        assert results[0]["sheet_name"] == "WithData"

    def test_all_sheets_header_only_raises(self, make_xlsx):
        """If ALL sheets have only headers and no data, raise ValueError."""
        path = make_xlsx(
            {
                "Sheet1": [
                    ["Year", "Value"],
                    # No data rows
                ],
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


# =============================================================================
# Test: Single data row (edge case for melt)
# =============================================================================


class TestSingleRowData:
    def test_single_data_row_melts_correctly(self, make_xlsx):
        """A sheet with exactly one data row produces correct melt output."""
        path = make_xlsx(
            {
                "Data": [
                    ["Region", "2020", "2021", "2022"],
                    ["NSW", 100, 200, 300],
                ],
            }
        )
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor="Region",
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
        )
        assert len(results) == 1
        df = results[0]["dataframe"]
        # 1 region × 3 value columns = 3 rows
        assert df.shape[0] == 3
        assert set(df.columns) == {"Region", "variable", "value"}
        # All rows should have Region == "NSW"
        assert df["Region"].unique().to_list() == ["NSW"]
        # Variables should be the year columns
        assert sorted(df["variable"].to_list()) == sorted(["2020", "2021", "2022"])


# =============================================================================
# Test: Trailing rows with partial non-null cells are NOT trimmed
# =============================================================================


class TestTrailingRowTrimming:
    def test_trailing_row_with_one_non_null_cell_not_trimmed(self, make_xlsx):
        """A trailing row with at least one non-null cell must NOT be removed."""
        path = make_xlsx(
            {
                "Data": [
                    ["Region", "2020", "2021"],
                    ["NSW", 100, 200],
                    ["VIC", 300, None],  # partial null — must NOT be trimmed
                    [None, None, None],  # fully null — SHOULD be trimmed
                ],
            }
        )
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor="Region",
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
        )
        assert len(results) == 1
        df = results[0]["dataframe"]
        # 2 data rows × 2 value columns = 4 rows after melt
        # The fully-null row is trimmed, but VIC row (partial null) stays
        assert df.shape[0] == 4
        # Verify VIC is present
        regions = df["Region"].unique().to_list()
        assert "VIC" in regions
        assert "NSW" in regions

    def test_trailing_row_single_non_null_in_value_column_not_trimmed(self, make_xlsx):
        """A trailing row where only a value column has data is preserved."""
        path = make_xlsx(
            {
                "Data": [
                    ["Region", "2020", "2021"],
                    ["NSW", 100, 200],
                    [None, 999, None],  # Region is null, but 2020 has data
                ],
            }
        )
        results = process_workbook_sheets(
            xlsx_path=path,
            anchor="Region",
            anchor_mode="exact",
            header_rows=1,
            id_column_count=1,
        )
        assert len(results) == 1
        df = results[0]["dataframe"]
        # 2 data rows × 2 value columns = 4 rows after melt
        assert df.shape[0] == 4


# =============================================================================
# Test: Non-default template_params propagation through the op
# =============================================================================


class TestNonDefaultTemplateParamsPropagation:
    """Op-level test proving non-default template_params drive processing.

    Uses anchor_match=contains, header_rows=2, id_column_count=2 — all
    non-default.  The XLSX data is designed so that default params would
    either fail to find the anchor or produce incorrect output shape.
    """

    def test_non_default_params_drive_processing(self, make_xlsx):
        """Non-default params (contains, 2 header rows, 2 ID cols) flow
        through the op and produce the expected child manifest shape.

        Failure modes if defaults were used instead:
        - anchor_match="exact": "Region" ≠ "Region Name" → ValueError (no anchor)
        - header_rows=1: would miss the category prefix row → wrong column names
        - id_column_count=1: would melt 3 value cols instead of 2 → wrong shape
        """
        from dagster import build_op_context

        # --- Build an XLSX that ONLY works with non-default params ---
        #
        # Layout:
        #   Row 0: ["Preamble line"]                               (junk)
        #   Row 1: ["", "Category", "Population", "Population"]    (header row 1)
        #   Row 2: ["Region Name", "Sub-Region", "Male", "Female"] (header row 2 — anchor)
        #   Row 3: ["NSW", "Sydney", 100, 200]                     (data)
        #   Row 4: ["VIC", "Melbourne", 300, 400]                  (data)
        #
        # anchor_text="Region", anchor_match="contains" → matches "Region Name"
        # header_rows=2 → header composed from rows 1+2
        # id_column_count=2 → "Region Name" + "Category Sub-Region" are IDs
        xlsx_path = make_xlsx(
            {
                "Data": [
                    ["Preamble line"],
                    ["", "Category", "Population", "Population"],
                    ["Region Name", "Sub-Region", "Male", "Female"],
                    ["NSW", "Sydney", 100, 200],
                    ["VIC", "Melbourne", 300, 400],
                ],
            }
        )

        manifest = {
            "batch_id": "batch_params_test",
            "uploader": "test_user",
            "intent": "ingest_complex_spreadsheet",
            "files": [
                {
                    "path": "s3://landing-zone/batch_params_test/data.xlsx",
                    "type": "tabular",
                    "format": "XLSX",
                }
            ],
            "metadata": {
                "title": "Params Test",
                "description": "Test non-default params",
                "keywords": ["test"],
                "source": "Unit Test",
                "license": "MIT",
                "attribution": "Test",
                "project": "TEST",
                "tags": {"dataset_id": "params_test_ds"},
                "complex_spreadsheet": {
                    "template_id": "anchor_unpivot_v1",
                    "template_params": {
                        "anchor_text": "Region",
                        "anchor_match": "contains",
                        "header_rows": 2,
                        "id_column_count": 2,
                    },
                },
            },
        }

        # --- Mock resources ---
        mock_minio = Mock()
        mock_minio.landing_bucket = "landing-zone"
        mock_minio.lake_bucket = "data-lake"

        def fake_download(s3_key, local_path):
            shutil.copy(xlsx_path, local_path)

        mock_minio.download_from_landing.side_effect = fake_download
        mock_minio.object_exists_in_landing.return_value = False
        mock_minio.upload_json_to_landing = Mock()

        mock_mongodb = Mock()
        mock_mongodb.get_run_object_id.return_value = "60a1f77bcf86cd799439022"

        context = build_op_context(
            resources={"minio": mock_minio, "mongodb": mock_mongodb},
        )

        # Mock register_intermediate_from_local_file to capture parquet content
        import polars as pl

        captured_dfs = []

        def capture_intermediate(
            *,
            local_path,
            batch_id,
            run_id,
            producer,
            label,
            parameters,
            content_type,
            minio,
            mongodb,
            log,
        ):
            df = pl.read_parquet(local_path)
            captured_dfs.append(df)
            return {
                "artifact_id": "art_123",
                "blob_s3_path": "s3://data-lake/blobs/test_hash",
            }

        with patch(
            "services.dagster.etl_pipelines.ops.complex_spreadsheet_ops."
            "register_intermediate_from_local_file",
            side_effect=capture_intermediate,
        ):
            result = split_complex_spreadsheet_op(context, manifest)

        # --- Assertions ---

        # 1. Op returns the original manifest unchanged
        assert result == manifest

        # 2. A child manifest was published (one sheet → one child)
        assert mock_minio.upload_json_to_landing.call_count == 1

        # 3. Inspect the child manifest content
        call_args = mock_minio.upload_json_to_landing.call_args
        child_key = call_args[0][0]  # positional arg 0: S3 key
        child_manifest = call_args[0][1]  # positional arg 1: manifest dict

        assert child_key == "manifests/batch_params_test__data.json"
        assert child_manifest["intent"] == "ingest_tabular"
        assert child_manifest["batch_id"] == "batch_params_test__data"
        assert child_manifest["files"][0]["format"] == "Parquet"

        # 4. Metadata propagated correctly
        child_meta = child_manifest["metadata"]
        assert child_meta["title"] == "Params Test — Data"
        assert child_meta["tags"]["parent_batch_id"] == "batch_params_test"
        assert child_meta["tags"]["source_sheet"] == "Data"

        # 5. Verify actual parquet content reflects non-default params
        assert len(captured_dfs) == 1, "Expected exactly one intermediate parquet"
        df = captured_dfs[0]
        # With header_rows=2, id_column_count=2:
        #   Composed headers: ["Region Name", "Category Sub-Region", "Population Male", "Population Female"]
        #   2 ID cols (Region Name, Category Sub-Region) + 2 value cols melted
        #   2 data rows × 2 value cols = 4 rows after melt
        #   Columns: Region Name, Category Sub-Region, variable, value
        assert df.shape == (4, 4), f"Expected (4, 4) but got {df.shape}"
        assert "Region Name" in df.columns
        assert "Category Sub-Region" in df.columns
        assert "variable" in df.columns
        assert "value" in df.columns


class TestSplitComplexSpreadsheetOpV2:
    def test_v2_regex_anchor_splits(self, make_xlsx):
        """v2 template dispatch supports regex anchor matching and v2 slugging."""
        from dagster import build_op_context

        xlsx_path = make_xlsx(
            {
                "Demographics 2024!": [
                    ["Preamble line"],
                    ["", "Population (%)", "Population (%)"],
                    ["REGION name", "Male", "Female"],
                    ["NSW", 100, 200],
                    ["VIC", 300, 400],
                ],
            }
        )

        manifest = {
            "batch_id": "batch_v2_test",
            "uploader": "test_user",
            "intent": "ingest_complex_spreadsheet",
            "files": [
                {
                    "path": "s3://landing-zone/batch_v2_test/data.xlsx",
                    "type": "tabular",
                    "format": "XLSX",
                }
            ],
            "metadata": {
                "title": "V2 Params Test",
                "description": "Test v2 regex dispatch",
                "keywords": ["test"],
                "source": "Unit Test",
                "license": "MIT",
                "attribution": "Test",
                "project": "TEST",
                "tags": {"dataset_id": "v2_test_ds"},
                "complex_spreadsheet": {
                    "template_id": "anchor_unpivot_v2",
                    "template_params": {
                        "anchor_text": r"region\s+name",
                        "anchor_match": "regex",
                        "header_rows": 2,
                        "id_column_count": 1,
                    },
                },
            },
        }

        mock_minio = Mock()
        mock_minio.landing_bucket = "landing-zone"
        mock_minio.lake_bucket = "data-lake"

        def fake_download(s3_key, local_path):
            shutil.copy(xlsx_path, local_path)

        mock_minio.download_from_landing.side_effect = fake_download
        mock_minio.object_exists_in_landing.return_value = False
        mock_minio.upload_json_to_landing = Mock()

        mock_mongodb = Mock()
        mock_mongodb.get_run_object_id.return_value = "60a1f77bcf86cd799439022"

        context = build_op_context(
            resources={"minio": mock_minio, "mongodb": mock_mongodb},
        )

        import polars as pl

        captured_dfs = []

        def capture_intermediate(
            *,
            local_path,
            batch_id,
            run_id,
            producer,
            label,
            parameters,
            content_type,
            minio,
            mongodb,
            log,
        ):
            df = pl.read_parquet(local_path)
            captured_dfs.append(df)
            return {
                "artifact_id": "art_456",
                "blob_s3_path": "s3://data-lake/blobs/test_hash_v2",
            }

        with patch(
            "services.dagster.etl_pipelines.ops.complex_spreadsheet_ops."
            "register_intermediate_from_local_file",
            side_effect=capture_intermediate,
        ):
            result = split_complex_spreadsheet_op(context, manifest)

        assert result == manifest
        assert mock_minio.upload_json_to_landing.call_count == 1

        call_args = mock_minio.upload_json_to_landing.call_args
        child_key = call_args[0][0]
        child_manifest = call_args[0][1]

        assert child_key == "manifests/batch_v2_test__demographics_2024.json"
        assert child_manifest["batch_id"] == "batch_v2_test__demographics_2024"
        assert (
            child_manifest["metadata"]["tags"]["dataset_id"]
            == "v2_test_ds__demographics_2024"
        )
        assert (
            child_manifest["metadata"]["tags"]["source_sheet"] == "Demographics 2024!"
        )

        assert len(captured_dfs) == 1
        df = captured_dfs[0]
        assert df.shape == (4, 3)
        assert "region_name" in df.columns
        assert "variable" in df.columns
        assert "value" in df.columns
        assert sorted(df["variable"].unique().to_list()) == sorted(
            ["population_male", "population_female"]
        )
