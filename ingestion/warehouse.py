"""Connections to the DuckDB warehouse file, with the session settings dbt uses too.

dbt sets the same values in dbt/profiles.yml. TimeZone matters most: timestamptz is shown
in the session's zone, and every reader of the warehouse works in UTC.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from ingestion.settings import warehouse_path

SESSION_SETTINGS = {
    "TimeZone": "UTC",
    "memory_limit": "1GiB",  # GiB: DuckDB reads GB as 10^9 bytes
    "threads": "4",
}


def connect(path: Path | None = None, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open the warehouse (DUCKDB_PATH by default). Its directory must exist: DuckDB creates
    the file but not the directory. One writer at a time: a second process, even a reader,
    fails on the file lock while a writer has it open."""
    path = warehouse_path() if path is None else path
    if not path.parent.is_dir():
        raise FileNotFoundError(f"Directory of the warehouse file not found: {path.parent}")
    return duckdb.connect(str(path), read_only=read_only, config=dict(SESSION_SETTINGS))
