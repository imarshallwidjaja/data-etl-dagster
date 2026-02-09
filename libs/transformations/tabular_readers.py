# =============================================================================
# Tabular Readers
# =============================================================================
# Format-specific readers that return PyArrow Tables.
# Each reader takes a file path and returns a pa.Table.
# =============================================================================

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

__all__ = ["read_csv_to_arrow", "read_parquet_to_arrow"]


def read_csv_to_arrow(path: str) -> pa.Table:
    """
    Read a CSV file into a PyArrow Table.

    Args:
        path: Local filesystem path to the CSV file.

    Returns:
        PyArrow Table with inferred schema.
    """
    return csv.read_csv(
        path,
        parse_options=csv.ParseOptions(delimiter=","),
        read_options=csv.ReadOptions(use_threads=True),
    )


def read_parquet_to_arrow(path: str) -> pa.Table:
    """
    Read a Parquet file into a PyArrow Table.

    Args:
        path: Local filesystem path to the Parquet file.

    Returns:
        PyArrow Table with schema from Parquet metadata.
    """
    return pq.read_table(path)
