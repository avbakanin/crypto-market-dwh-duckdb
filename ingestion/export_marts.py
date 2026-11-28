"""Export the marts the dashboard reads from the DuckDB warehouse to local Parquet files.

Run from repo root: python -m ingestion.export_marts
In Airflow it is the last task of the pipeline, after dbt has built and checked the marts.

    data/marts/<export_id>/<table>.parquet   one file per exported table
    data/marts/<export_id>/_manifest.json    written last: marks the export complete

The dashboard reads the newest complete export and never the warehouse file: DuckDB lets
one process write a file, and while a writer has it, even a reader in another container
fails on the lock. Each export is a new directory (named like a landing-zone batch: its
UTC as-of moment), so a reader never sees half a set; the newest KEEP_EXPORTS complete
exports are kept. The files keep the contract of the Snowflake original's export: the
same tables, columns and type rules (see arrow_type).

Opens the warehouse read-only (DUCKDB_PATH): the export cannot change it, and no writer
can change it while the export runs.
"""

from __future__ import annotations

import json
import re
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.batches import MANIFEST_NAME, batch_id_for, complete_batches, is_batch_id
from ingestion.settings import PROJECT_ROOT, warehouse_path
from ingestion.warehouse import connect

OUTPUT_DIR = PROJECT_ROOT / "data" / "marts"
KEEP_EXPORTS = 8  # a day of exports at the pipeline's 3-hour cadence
SOURCE = "duckdb"  # the manifest says which engine built the marts


@dataclass(frozen=True)
class MartExport:
    """One exported table. Fixed names, never user input: they go into SQL as they are."""

    name: str  # file name, without .parquet
    relation: str
    order_by: str  # a stable row order: the same data gives the same file


EXPORTS = (
    MartExport("dim_symbol", "marts.dim_symbol", "symbol"),
    MartExport("fct_pump_events", "marts.fct_pump_events", "event_open_time_ms, symbol"),
    MartExport(
        "fct_pump_event_window", "marts.fct_pump_event_window", "pump_event_id, offset_hours"
    ),
    MartExport("mart_pump_profile", "marts.mart_pump_profile", "event_scope, metric, offset_hours"),
    # Per-event numbers of the profile and the next-hour check: the analysis resamples
    # event days from them (analysis/pump_findings.py).
    MartExport(
        "fct_pump_event_excess",
        "marts.fct_pump_event_excess",
        "pump_event_id, metric, offset_hours",
    ),
    MartExport("mart_pump_signal_check", "marts.mart_pump_signal_check", "rule, signal_date"),
    MartExport(
        "dq_check_results",
        "dq.dq_check_results",
        "run_started_at, invocation_id, check_unique_id",
    ),
    # How far each dataset reaches: the dashboard's "data as of".
    MartExport("dq_data_coverage", "dq.dq_data_coverage", "dataset"),
    # The forward test (signals/): decisions with outcomes, results per version.
    MartExport("fct_signal_log", "marts.fct_signal_log", "decision_date, version"),
    MartExport("mart_signal_performance", "marts.mart_signal_performance", "version"),
)

INTEGER_TYPES = frozenset({"TINYINT", "SMALLINT", "INTEGER", "BIGINT"})
DECIMAL_TYPE = re.compile(r"DECIMAL\((\d+),(\d+)\)")
SIMPLE_TYPES = {
    "FLOAT": pa.float64(),
    "DOUBLE": pa.float64(),
    "VARCHAR": pa.string(),
    "BOOLEAN": pa.bool_(),
    "DATE": pa.date32(),
    "TIMESTAMP WITH TIME ZONE": pa.timestamp("us", tz="UTC"),
    "TIMESTAMP": pa.timestamp("us"),
}


def arrow_type(column: str, duckdb_type: str) -> pa.DataType:
    """The Parquet type of a column, from its DuckDB type.

    The rules of the Snowflake export: every integer is int64 (DuckDB hands an INTEGER
    over as int32), a decimal without a scale is an integer, other decimals stay
    decimals, FLOAT widens to double; strings, booleans, dates and timestamps stay
    (TIMESTAMP WITH TIME ZONE in microseconds, UTC). Any other type fails: count_if
    gives a HUGEINT, which DuckDB's own Parquet writer turns into a double without a
    word, so a new column type in a mart must be a decision - usually a cast in the
    model - not a surprise in the dashboard.
    """
    if duckdb_type in INTEGER_TYPES:
        return pa.int64()
    decimal = DECIMAL_TYPE.fullmatch(duckdb_type)
    if decimal:
        precision, scale = int(decimal[1]), int(decimal[2])
        return pa.int64() if scale == 0 else pa.decimal128(precision, scale)
    if duckdb_type in SIMPLE_TYPES:
        return SIMPLE_TYPES[duckdb_type]
    raise ValueError(
        f"Column {column}: DuckDB type {duckdb_type} is not exported (cast it in the model)"
    )


def table_columns(con: duckdb.DuckDBPyConnection, export: MartExport) -> list[tuple[str, str]]:
    """(name, DuckDB type) of each column, in the table's order."""
    rows = con.execute(f"DESCRIBE {export.relation}").fetchall()
    return [(row[0], row[1]) for row in rows]


def target_schema(columns: list[tuple[str, str]]) -> pa.Schema:
    """Every field nullable, as the Snowflake export wrote them. DuckDB's COPY ... TO
    parquet marks a column required when the written rows happen to have no NULL, so
    the schema would change with the data."""
    return pa.schema([pa.field(name, arrow_type(name, kind)) for name, kind in columns])


def conform(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Cast the result to the schema; a lossy cast (an overflow, lost digits) fails."""
    return table.cast(schema, safe=True)


def select_sql(export: MartExport) -> str:
    return f"SELECT * FROM {export.relation} ORDER BY {export.order_by}"


def export_table(con: duckdb.DuckDBPyConnection, export: MartExport, path: Path) -> dict[str, Any]:
    """Write one table and describe the file as written: rows and Parquet column types.
    The file's rows must equal the table's: the query may not lose any."""
    schema = target_schema(table_columns(con, export))
    pq.write_table(conform(con.sql(select_sql(export)).to_arrow_table(), schema), path)
    expected = con.execute(f"SELECT count(*) FROM {export.relation}").fetchall()[0][0]
    rows = pq.read_metadata(path).num_rows
    if rows != expected:
        raise RuntimeError(f"{export.relation}: {rows} rows written, the table has {expected}")
    return {
        "relation": export.relation,
        "rows": rows,
        "columns": {field.name: str(field.type) for field in pq.read_schema(path)},
    }


def write_export(
    con: duckdb.DuckDBPyConnection, export_dir: Path, as_of: datetime, database: str
) -> dict[str, Any]:
    """Write every table of EXPORTS into export_dir, the manifest last; return the manifest.

    The directory must not exist yet. A failure leaves it without a manifest: incomplete,
    invisible to readers, removed by a later prune.
    """
    export_dir.mkdir(parents=True)
    tables: dict[str, Any] = {}
    for export in EXPORTS:
        path = export_dir / f"{export.name}.parquet"
        tables[export.name] = export_table(con, export, path)
        print(f"  {export.name}: {tables[export.name]['rows']} rows, {path.stat().st_size} bytes")
    manifest = {
        "export_id": export_dir.name,
        "exported_at_utc": as_of.isoformat(),
        "source": SOURCE,
        "database": database,  # lineage: which warehouse file
        "tables": tables,
    }
    (export_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def stale_exports(root: Path, keep: int) -> list[Path]:
    """Export directories to remove: complete ones beyond the newest `keep`, and incomplete
    ones older than the newest complete export (an incomplete one newer than that may be
    an export in progress). Anything not named like an export is left alone."""
    complete = complete_batches(root)
    if not complete:
        return []
    kept = {d.name for d in complete[-keep:]} if keep > 0 else set()
    newest = complete[-1].name
    return sorted(
        d
        for d in root.iterdir()
        if d.is_dir() and is_batch_id(d.name) and d.name not in kept and d.name < newest
    )


def prune_exports(root: Path, keep: int = KEEP_EXPORTS) -> list[Path]:
    stale = stale_exports(root, keep)
    for directory in stale:
        shutil.rmtree(directory)
    return stale


def main(now_ms: int | None = None) -> int:
    root = OUTPUT_DIR
    if now_ms is None:
        now_ms = time.time_ns() // 1_000_000
    as_of = datetime.fromtimestamp(now_ms / 1000, tz=UTC)
    export_dir = root / batch_id_for(now_ms)
    if export_dir.exists():  # before opening the warehouse, as for a landing-zone batch
        raise FileExistsError(f"Export already exists: {export_dir}")
    database = warehouse_path()
    if not database.is_file():  # read-only: DuckDB would not create it, but fail late
        print(f"FAIL: warehouse file not found: {database}; nothing exported")
        return 1

    print(f"Export {export_dir.name} from {database.name} -> {export_dir}")
    with connect(database, read_only=True) as con:
        manifest = write_export(con, export_dir, as_of, database.name)

    removed = prune_exports(root)
    total = sum(t["rows"] for t in manifest["tables"].values())
    print(
        f"OK: exported {len(manifest['tables'])} tables, {total} rows; "
        f"removed {len(removed)} old exports"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
