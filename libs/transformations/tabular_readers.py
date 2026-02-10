"""Tabular file readers returning Arrow tables."""

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq


def read_csv_to_arrow(path: str) -> pa.Table:
    """Read a CSV file into an Arrow table."""
    return csv.read_csv(
        path,
        parse_options=csv.ParseOptions(delimiter=","),
        read_options=csv.ReadOptions(use_threads=True),
    )


def read_parquet_to_arrow(path: str) -> pa.Table:
    """Read a Parquet file into an Arrow table."""
    return pq.read_table(path)
