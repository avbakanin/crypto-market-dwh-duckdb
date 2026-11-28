"""Tests for the export of the marts to Parquet: a real DuckDB file with small tables in
the exported relations, temporary directories."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion import export_marts
from ingestion.batches import MANIFEST_NAME, batch_id_for
from ingestion.export_marts import (
    EXPORTS,
    MartExport,
    arrow_type,
    select_sql,
    stale_exports,
    write_export,
)
from ingestion.warehouse import connect

AS_OF = datetime(2026, 11, 24, 9, 10, tzinfo=UTC)
# Every column type of the marts, as DuckDB declares them.
VALUE_COLUMNS = (
    ("id", "VARCHAR"),
    ("n", "INTEGER"),
    ("x", "DECIMAL(38,8)"),
    ("d0", "DECIMAL(38,0)"),
    ("f", "FLOAT"),
    ("ts", "TIMESTAMP WITH TIME ZONE"),
    ("flag", "BOOLEAN"),
    ("day", "DATE"),
)


def order_columns(export: MartExport) -> list[str]:
    return [c.strip() for c in export.order_by.split(",")]


def create_mart(con: duckdb.DuckDBPyConnection, export: MartExport, rows: int) -> None:
    """A table with the export's sort columns (BIGINT) and one column of every type; rows
    inserted in reverse order, so only the export's ORDER BY sorts them."""
    keys = order_columns(export)
    columns = [(k, "BIGINT") for k in keys] + list(VALUE_COLUMNS)
    con.execute(f"CREATE TABLE {export.relation} ({', '.join(f'{c} {t}' for c, t in columns)})")
    for i in reversed(range(rows)):
        con.execute(
            f"INSERT INTO {export.relation} VALUES ({', '.join('?' for _ in columns)})",
            [i] * len(keys)
            + [f"id-{i}", i, "1.12345678", 10**9 + i, 0.5, AS_OF, True, AS_OF.date()],
        )


@pytest.fixture
def warehouse(tmp_path: Path) -> Path:
    path = tmp_path / "warehouse" / "test.duckdb"
    path.parent.mkdir()
    with connect(path) as con:
        con.execute("CREATE SCHEMA marts; CREATE SCHEMA dq")
        for export in EXPORTS:
            create_mart(con, export, rows=3)
    return path


@pytest.fixture
def con(warehouse: Path) -> Any:
    with connect(warehouse, read_only=True) as connection:
        yield connection


def file_schema(export: MartExport) -> pa.Schema:
    keys = [pa.field(k, pa.int64()) for k in order_columns(export)]
    return pa.schema(
        [
            *keys,
            pa.field("id", pa.string()),
            pa.field("n", pa.int64()),
            pa.field("x", pa.decimal128(38, 8)),
            pa.field("d0", pa.int64()),
            pa.field("f", pa.float64()),
            pa.field("ts", pa.timestamp("us", tz="UTC")),
            pa.field("flag", pa.bool_()),
            pa.field("day", pa.date32()),
        ]
    )


# --- types ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("duckdb_type", "expected"),
    [
        ("TINYINT", pa.int64()),
        ("SMALLINT", pa.int64()),
        ("INTEGER", pa.int64()),
        ("BIGINT", pa.int64()),
        ("DECIMAL(38,0)", pa.int64()),
        ("DECIMAL(18,0)", pa.int64()),
        ("DECIMAL(38,8)", pa.decimal128(38, 8)),
        ("DECIMAL(38,12)", pa.decimal128(38, 12)),
        ("DECIMAL(10,2)", pa.decimal128(10, 2)),
        ("FLOAT", pa.float64()),
        ("DOUBLE", pa.float64()),
        ("VARCHAR", pa.string()),
        ("BOOLEAN", pa.bool_()),
        ("DATE", pa.date32()),
        ("TIMESTAMP", pa.timestamp("us")),
        ("TIMESTAMP WITH TIME ZONE", pa.timestamp("us", tz="UTC")),
    ],
)
def test_arrow_type_follows_the_rules_of_the_original_export(
    duckdb_type: str, expected: pa.DataType
) -> None:
    assert arrow_type("c", duckdb_type) == expected


@pytest.mark.parametrize(
    "duckdb_type",
    ["HUGEINT", "UBIGINT", "JSON", "TIMESTAMP_NS", "INTERVAL", "DECIMAL(38,8)[]", "VARCHAR[]"],
)
def test_a_column_type_the_export_does_not_know_is_refused(duckdb_type: str) -> None:
    with pytest.raises(ValueError, match="followed_by_pump"):
        arrow_type("followed_by_pump", duckdb_type)


def test_files_have_one_stable_schema_per_column_type(con: Any, tmp_path: Path) -> None:
    export_dir = tmp_path / "20261124T091000Z"
    write_export(con, export_dir, AS_OF, "test.duckdb")
    for export in EXPORTS:
        schema = pq.read_schema(export_dir / f"{export.name}.parquet")
        assert schema == file_schema(export)
        assert all(field.nullable for field in schema)  # no NULL in the rows, still nullable


def test_a_lossy_cast_fails_instead_of_rounding(tmp_path: Path) -> None:
    path = tmp_path / "w.duckdb"
    with connect(path) as con:
        con.execute("CREATE SCHEMA marts; CREATE SCHEMA dq")
        for export in EXPORTS:
            create_mart(con, export, rows=1)
        con.execute("UPDATE marts.dim_symbol SET d0 = 10::DECIMAL(38,0) ^ 20")  # > int64
        with pytest.raises(pa.ArrowInvalid):
            write_export(con, tmp_path / "20261124T091000Z", AS_OF, "w.duckdb")


def test_timestamps_are_written_in_utc_whatever_the_session_zone(tmp_path: Path) -> None:
    with connect(tmp_path / "w.duckdb") as con:
        con.execute("CREATE SCHEMA marts; CREATE SCHEMA dq; SET TimeZone = 'Asia/Tokyo'")
        for export in EXPORTS:
            create_mart(con, export, rows=1)
        write_export(con, tmp_path / "20261124T091000Z", AS_OF, "w.duckdb")
    ts = pq.read_table(tmp_path / "20261124T091000Z" / "dim_symbol.parquet").column("ts")
    assert ts.type == pa.timestamp("us", tz="UTC")
    assert ts.to_pylist() == [AS_OF]


# --- queries ----------------------------------------------------------------------------


def test_exported_files_are_the_ones_the_readers_expect() -> None:
    """Independent literals: renaming a file breaks the dashboard or the analysis."""
    assert [export.name for export in EXPORTS] == [
        "dim_symbol",
        "fct_pump_events",
        "fct_pump_event_window",
        "mart_pump_profile",
        "fct_pump_event_excess",
        "mart_pump_signal_check",
        "dq_check_results",
        "dq_data_coverage",
        "fct_signal_log",
        "mart_signal_performance",
    ]
    assert {export.relation.split(".")[0] for export in EXPORTS} == {"marts", "dq"}
    assert all(export.relation.endswith("." + export.name) for export in EXPORTS)


def test_every_query_has_a_stable_row_order() -> None:
    assert select_sql(EXPORTS[0]) == "SELECT * FROM marts.dim_symbol ORDER BY symbol"
    assert all(" ORDER BY " in select_sql(export) for export in EXPORTS)


def test_rows_are_written_in_the_order_of_the_export(con: Any, tmp_path: Path) -> None:
    export_dir = tmp_path / "20261124T091000Z"
    write_export(con, export_dir, AS_OF, "test.duckdb")
    for export in EXPORTS:
        table = pq.read_table(export_dir / f"{export.name}.parquet")
        assert table.column("id").to_pylist() == ["id-0", "id-1", "id-2"], export.name


# --- one export -------------------------------------------------------------------------


def test_export_writes_every_table_and_the_manifest(con: Any, tmp_path: Path) -> None:
    export_dir = tmp_path / "20261124T091000Z"
    manifest = write_export(con, export_dir, AS_OF, "test.duckdb")

    assert sorted(p.name for p in export_dir.iterdir()) == sorted(
        [f"{export.name}.parquet" for export in EXPORTS] + [MANIFEST_NAME]
    )
    assert json.loads((export_dir / MANIFEST_NAME).read_text(encoding="utf-8")) == manifest
    assert manifest["export_id"] == "20261124T091000Z"
    assert manifest["exported_at_utc"] == "2026-11-24T09:10:00+00:00"
    assert manifest["source"] == "duckdb"
    assert manifest["database"] == "test.duckdb"
    entry = manifest["tables"]["fct_pump_events"]
    assert entry["relation"] == "marts.fct_pump_events"
    assert entry["rows"] == 3
    assert entry["columns"]["n"] == "int64"
    assert entry["columns"]["ts"] == "timestamp[us, tz=UTC]"


def test_an_empty_mart_is_exported_with_its_columns(con: Any, tmp_path: Path) -> None:
    path = tmp_path / "w.duckdb"
    with connect(path) as writable:
        writable.execute("CREATE SCHEMA marts; CREATE SCHEMA dq")
        for export in EXPORTS:
            create_mart(writable, export, rows=0 if export.name == "fct_pump_events" else 2)
        export_dir = tmp_path / "20261124T091000Z"
        manifest = write_export(writable, export_dir, AS_OF, "w.duckdb")
    table = pq.read_table(export_dir / "fct_pump_events.parquet")
    assert table.num_rows == 0
    assert table.schema == file_schema(EXPORTS[1])
    assert manifest["tables"]["fct_pump_events"]["rows"] == 0


def test_rows_lost_between_the_table_and_the_file_fail_the_export(
    con: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = export_marts.select_sql
    monkeypatch.setattr(export_marts, "select_sql", lambda e: original(e) + " LIMIT 2")
    export_dir = tmp_path / "20261124T091000Z"
    with pytest.raises(RuntimeError, match="2 rows written, the table has 3"):
        write_export(con, export_dir, AS_OF, "test.duckdb")
    assert not (export_dir / MANIFEST_NAME).exists()


def test_a_failed_export_leaves_no_manifest(tmp_path: Path) -> None:
    path = tmp_path / "w.duckdb"
    with connect(path) as con:
        con.execute("CREATE SCHEMA marts; CREATE SCHEMA dq")
        for export in EXPORTS:
            if export is not EXPORTS[2]:
                create_mart(con, export, rows=1)
        export_dir = tmp_path / "20261124T091000Z"
        with pytest.raises(duckdb.CatalogException, match="fct_pump_event_window"):
            write_export(con, export_dir, AS_OF, "w.duckdb")
    assert sorted(p.name for p in export_dir.iterdir()) == [
        "dim_symbol.parquet",
        "fct_pump_events.parquet",
    ]


def test_an_existing_export_directory_is_never_overwritten(con: Any, tmp_path: Path) -> None:
    export_dir = tmp_path / "20261124T091000Z"
    export_dir.mkdir()
    with pytest.raises(FileExistsError):
        write_export(con, export_dir, AS_OF, "test.duckdb")
    assert list(export_dir.iterdir()) == []


# --- retention --------------------------------------------------------------------------


def make_export(root: Path, export_id: str, complete: bool = True) -> Path:
    directory = root / export_id
    directory.mkdir(parents=True)
    (directory / "dim_symbol.parquet").write_bytes(b"PAR1")
    if complete:
        (directory / MANIFEST_NAME).write_text("{}", encoding="utf-8")
    return directory


def test_old_complete_exports_beyond_the_newest_ones_are_stale(tmp_path: Path) -> None:
    ids = ["20261124T000700Z", "20261124T030700Z", "20261124T060700Z", "20261124T090700Z"]
    for export_id in ids:
        make_export(tmp_path, export_id)
    assert [d.name for d in stale_exports(tmp_path, keep=2)] == ids[:2]


def test_incomplete_exports_are_stale_only_when_older_than_the_newest_complete(
    tmp_path: Path,
) -> None:
    make_export(tmp_path, "20261124T030700Z", complete=False)  # failed export
    make_export(tmp_path, "20261124T060700Z")
    make_export(tmp_path, "20261124T090700Z", complete=False)  # maybe still being written
    (tmp_path / "2026-11-24-backup").mkdir()  # a manual copy, sorts first: never touched
    (tmp_path / "20261124T010700Z.txt").write_text("x", encoding="utf-8")
    assert [d.name for d in stale_exports(tmp_path, keep=8)] == ["20261124T030700Z"]


def test_nothing_is_stale_without_a_complete_export(tmp_path: Path) -> None:
    make_export(tmp_path, "20261124T030700Z", complete=False)
    assert stale_exports(tmp_path, keep=8) == []
    assert stale_exports(tmp_path / "missing", keep=8) == []


# --- main -------------------------------------------------------------------------------

NOW_MS = int(AS_OF.timestamp() * 1000)


@pytest.fixture
def output_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "marts"
    monkeypatch.setattr(export_marts, "OUTPUT_DIR", root)
    return root


def record_connect(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Path, bool]]:
    calls: list[tuple[Path, bool]] = []

    def recording_connect(path: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        calls.append((path, read_only))
        return connect(path, read_only=read_only)

    monkeypatch.setattr(export_marts, "connect", recording_connect)
    return calls


def test_main_exports_read_only_and_prunes_old_exports(
    warehouse: Path, output_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DUCKDB_PATH", str(warehouse))
    for hour in range(8):
        make_export(output_dir, f"20261123T{hour:02d}0700Z")
    calls = record_connect(monkeypatch)

    assert export_marts.main(now_ms=NOW_MS) == 0
    assert calls == [(warehouse, True)]
    exports = sorted(d.name for d in output_dir.iterdir())
    assert len(exports) == export_marts.KEEP_EXPORTS
    assert exports[0] == "20261123T010700Z"  # the oldest one is gone
    assert exports[-1] == batch_id_for(NOW_MS) == "20261124T091000Z"
    manifest = json.loads((output_dir / exports[-1] / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert manifest["database"] == "test.duckdb"
    assert manifest["exported_at_utc"] == "2026-11-24T09:10:00+00:00"


def test_main_refuses_a_missing_warehouse_and_writes_nothing(
    tmp_path: Path, output_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "missing.duckdb"))
    calls = record_connect(monkeypatch)
    assert export_marts.main(now_ms=NOW_MS) == 1
    assert calls == []
    assert not output_dir.exists()
    assert not (tmp_path / "missing.duckdb").exists()


def test_main_refuses_an_existing_export_before_opening_the_warehouse(
    warehouse: Path, output_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DUCKDB_PATH", str(warehouse))
    make_export(output_dir, batch_id_for(NOW_MS))
    calls = record_connect(monkeypatch)
    with pytest.raises(FileExistsError):
        export_marts.main(now_ms=NOW_MS)
    assert calls == []
