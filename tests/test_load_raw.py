"""Tests for the RAW loader on a real DuckDB: lineage, the guard, reconciliation in one
transaction, batch order, the stop at the first batch that does not reconcile, main."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ingestion import load_raw
from ingestion.batches import Manifest, write_manifest
from ingestion.datasets import DATASETS, FUNDING_RATE, KLINES, RawDataset
from ingestion.load_raw import (
    LoadError,
    batch_stats,
    check_symbol,
    ensure_raw_objects,
    load_batch,
    load_pending,
    pending_batches,
    read_local_files,
    reconcile,
)
from ingestion.warehouse import connect
from tests.factories import START_MS, make_funding, make_klines

HOUR_MS = 3_600_000
B1, B2, B3 = "20250102T000000Z", "20250102T030000Z", "20250102T060000Z"
SYMBOLS = ("BTCUSDT", "ETHUSDT")
ROWS = 24


def write_batch(
    root: Path,
    batch_id: str,
    records: dict[str, list[Any]],
    duplicates: dict[str, int] | None = None,
) -> Path:
    """A complete batch: one JSON file per symbol, the manifest last."""
    batch_dir = root / batch_id
    batch_dir.mkdir(parents=True)
    for symbol, rows in records.items():
        (batch_dir / f"{symbol}.json").write_text(json.dumps(rows), encoding="utf-8")
    manifest: Manifest = {
        "batch_id": batch_id,
        "as_of_utc": "2025-01-02T00:00:00+00:00",
        "interval": "1h",
        "symbols": {
            s: {"rows": len(rows), "duplicates": (duplicates or {}).get(s, 0), "gaps": 0}
            for s, rows in records.items()
        },
    }
    write_manifest(batch_dir, manifest)
    return batch_dir


def klines(start_hour: int = 0, count: int = ROWS) -> dict[str, list[Any]]:
    return {s: make_klines(count, START_MS + start_hour * HOUR_MS) for s in SYMBOLS}


def manifest_of(batch_dir: Path) -> Manifest:
    return json.loads((batch_dir / "_manifest.json").read_text(encoding="utf-8"))


def count(con: duckdb.DuckDBPyConnection, table: str, batch_id: str | None = None) -> int:
    sql = f"SELECT count(*) FROM {table}" + (" WHERE _batch_id = ?" if batch_id else "")
    row = con.execute(sql, [batch_id] if batch_id else []).fetchone()
    assert row is not None
    return row[0]


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    with duckdb.connect(config={"TimeZone": "UTC"}) as connection:
        ensure_raw_objects(connection)
        yield connection


@pytest.fixture
def klines_root(tmp_path: Path) -> Path:
    return tmp_path / "raw" / "klines_1h"


@pytest.fixture
def ds(klines_root: Path) -> RawDataset:
    return dataclasses.replace(KLINES, landing_dir=klines_root)


# --- inputs ---------------------------------------------------------------------------------


@pytest.mark.parametrize("symbol", ["BTCUSDT", "1000PEPEUSDT", "币安人生USDT"])
def test_symbol_names_of_the_exchange_are_accepted(symbol: str) -> None:
    assert check_symbol(symbol) == symbol


@pytest.mark.parametrize(
    "symbol",
    [
        "BTCUSDT.json",
        "../ETHUSDT",
        "BTC/USDT",
        "btcusdt",
        "",
        "BTC_250328",
        "币安。USDT",
        "BTC USDT",
    ],
)
def test_symbol_that_could_leave_the_batch_directory_is_refused(symbol: str) -> None:
    with pytest.raises(LoadError, match="Unexpected symbol name"):
        check_symbol(symbol)


def test_missing_file_is_refused_before_any_write(
    con: duckdb.DuckDBPyConnection, klines_root: Path, ds: RawDataset
) -> None:
    batch_dir = write_batch(klines_root, B1, klines())
    (batch_dir / "ETHUSDT.json").unlink()
    with pytest.raises(LoadError, match="ETHUSDT.json is missing"):
        load_batch(con, ds, batch_dir, manifest_of(batch_dir))
    assert count(con, ds.table) == 0


def test_file_changed_after_the_extract_is_refused_before_any_write(
    con: duckdb.DuckDBPyConnection, klines_root: Path, ds: RawDataset
) -> None:
    batch_dir = write_batch(klines_root, B1, klines())
    (batch_dir / "BTCUSDT.json").write_text(json.dumps(make_klines(ROWS - 1)), encoding="utf-8")
    with pytest.raises(LoadError, match="BTCUSDT.json has 23 rows, manifest says 24"):
        read_local_files(batch_dir, manifest_of(batch_dir))
    with pytest.raises(LoadError):
        load_batch(con, ds, batch_dir, manifest_of(batch_dir))
    assert count(con, ds.table) == 0


def test_files_outside_the_manifest_are_not_loaded(
    con: duckdb.DuckDBPyConnection, klines_root: Path, ds: RawDataset
) -> None:
    batch_dir = write_batch(klines_root, B1, klines())
    (batch_dir / "XRPUSDT.json").write_text(json.dumps(make_klines(3)), encoding="utf-8")
    assert load_batch(con, ds, batch_dir, manifest_of(batch_dir)) == []
    assert count(con, ds.table) == 2 * ROWS


# --- reconciliation (pure) ------------------------------------------------------------------


def manifest_with(rows: int = ROWS, duplicates: int = 0) -> Manifest:
    return {
        "batch_id": B1,
        "as_of_utc": "",
        "interval": "1h",
        "symbols": {s: {"rows": rows, "duplicates": duplicates, "gaps": 0} for s in SYMBOLS},
    }


def stats_with(rows: int = ROWS, duplicates: int = 0) -> dict[str, dict[str, int]]:
    return {s: {"rows": rows, "duplicates": duplicates} for s in SYMBOLS}


def test_batch_as_in_the_manifest_reconciles() -> None:
    assert reconcile(stats_with(), manifest_with()) == []


def test_row_count_differing_from_the_manifest_is_a_problem() -> None:
    assert reconcile(stats_with(rows=23), manifest_with()) == [
        "BTCUSDT: 23 rows, manifest 24",
        "ETHUSDT: 23 rows, manifest 24",
    ]


def test_duplicates_added_by_the_load_are_a_problem() -> None:
    assert reconcile(stats_with(duplicates=1), manifest_with()) == [
        "BTCUSDT: 1 duplicate keys, manifest 0",
        "ETHUSDT: 1 duplicate keys, manifest 0",
    ]


def test_duplicates_already_in_the_source_file_reconcile() -> None:
    assert reconcile(stats_with(duplicates=2), manifest_with(duplicates=2)) == []


def test_symbol_missing_from_the_table_or_the_manifest_is_a_problem() -> None:
    stats = {"BTCUSDT": {"rows": ROWS, "duplicates": 0}, "XRPUSDT": {"rows": 1, "duplicates": 0}}
    assert reconcile(stats, manifest_with()) == [
        "ETHUSDT: not in table",
        "XRPUSDT: not in manifest",
    ]


# --- one batch ------------------------------------------------------------------------------


def test_every_record_is_loaded_as_it_came_with_its_lineage(
    con: duckdb.DuckDBPyConnection, klines_root: Path, ds: RawDataset
) -> None:
    batch_dir = write_batch(klines_root, B1, klines())
    assert load_batch(con, ds, batch_dir, manifest_of(batch_dir)) == []

    rows = con.execute(
        f"SELECT symbol, payload::VARCHAR, _batch_id, _source_file, _file_row_number,"
        f" _file_content_key, _loaded_at FROM {ds.table}"
        " WHERE symbol = 'BTCUSDT' ORDER BY _file_row_number"
    ).fetchall()
    text = (batch_dir / "BTCUSDT.json").read_text(encoding="utf-8")
    assert [json.loads(r[1]) for r in rows] == json.loads(text)  # each record, in file order
    assert [r[4] for r in rows] == list(range(1, ROWS + 1))
    assert {r[0] for r in rows} == {"BTCUSDT"}
    assert {(r[2], r[3]) for r in rows} == {(B1, f"{B1}/BTCUSDT.json")}
    assert {r[5] for r in rows} == {hashlib.md5(text.encode()).hexdigest()}
    assert {r[6].utcoffset() for r in rows} == {timedelta(0)}


def test_batch_already_in_the_table_is_not_inserted_again(
    con: duckdb.DuckDBPyConnection, klines_root: Path, ds: RawDataset
) -> None:
    batch_dir = write_batch(klines_root, B1, klines())
    assert load_batch(con, ds, batch_dir, manifest_of(batch_dir)) == []
    assert load_batch(con, ds, batch_dir, manifest_of(batch_dir)) == []
    assert count(con, ds.table) == 2 * ROWS


def test_batch_that_does_not_reconcile_is_rolled_back(
    con: duckdb.DuckDBPyConnection, klines_root: Path, ds: RawDataset
) -> None:
    records = klines()
    records["BTCUSDT"].append(records["BTCUSDT"][5])  # a duplicate the manifest does not list
    batch_dir = write_batch(klines_root, B1, records)
    assert load_batch(con, ds, batch_dir, manifest_of(batch_dir)) == [
        "BTCUSDT: 1 duplicate keys, manifest 0"
    ]
    assert count(con, ds.table) == 0  # not even the symbol that reconciled


def test_failure_half_way_leaves_nothing_and_the_connection_usable(
    con: duckdb.DuckDBPyConnection, klines_root: Path, ds: RawDataset
) -> None:
    batch_dir = write_batch(klines_root, B1, klines())
    broken = dataclasses.replace(ds, key_sql="no_such_column")  # fails after the insert
    with pytest.raises(duckdb.Error):
        load_batch(con, broken, batch_dir, manifest_of(batch_dir))
    assert count(con, ds.table) == 0
    assert load_batch(con, ds, batch_dir, manifest_of(batch_dir)) == []


def test_funding_duplicates_are_counted_by_hour(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    # fundingTime arrives a few ms after the hour; the extract counts duplicates per hour.
    rows = [make_funding("BTCUSDT", START_MS + 3), make_funding("BTCUSDT", START_MS + 7)]
    root = tmp_path / "funding_rate"
    batch_dir = write_batch(root, B1, {"BTCUSDT": rows}, duplicates={"BTCUSDT": 1})
    ds = dataclasses.replace(FUNDING_RATE, landing_dir=root)
    assert load_batch(con, ds, batch_dir, manifest_of(batch_dir)) == []
    assert batch_stats(con, ds, B1) == {"BTCUSDT": {"rows": 2, "duplicates": 1}}


def test_a_year_of_klines_loads_within_a_small_memory_limit(
    klines_root: Path, ds: RawDataset
) -> None:
    # A lateral join of read_text and json_each over the year-long first batch ran out of
    # 1 GiB; one statement per file needs little more than the file (note 106).
    records = {s: make_klines(8760) for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")}
    batch_dir = write_batch(klines_root, B1, records)
    with duckdb.connect(config={"memory_limit": "64MB", "threads": "4"}) as small:
        ensure_raw_objects(small)
        assert load_batch(small, ds, batch_dir, manifest_of(batch_dir)) == []
        assert count(small, ds.table) == 4 * 8760


def test_every_dataset_has_a_raw_table(con: duckdb.DuckDBPyConnection) -> None:
    tables = {
        f"{schema}.{name}"
        for schema, name in con.execute(
            "SELECT schema_name, table_name FROM duckdb_tables()"
        ).fetchall()
    }
    assert tables == {d.table for d in DATASETS}


# --- many batches ---------------------------------------------------------------------------


def test_pending_batches_are_oldest_first_without_loaded_or_incomplete_ones(
    klines_root: Path,
) -> None:
    for batch_id in (B3, B1, B2):
        write_batch(klines_root, batch_id, klines())
    (klines_root / "20250102T090000Z").mkdir()  # extract still writing: no manifest yet
    assert [d.name for d in pending_batches(klines_root, set())] == [B1, B2, B3]
    assert [d.name for d in pending_batches(klines_root, {B2})] == [B1, B3]


def test_until_includes_its_own_batch_and_nothing_newer(klines_root: Path) -> None:
    for batch_id in (B1, B2, B3):
        write_batch(klines_root, batch_id, klines())
    assert [d.name for d in pending_batches(klines_root, set(), until=B2)] == [B1, B2]


def test_load_stops_at_the_first_batch_that_does_not_reconcile(
    con: duckdb.DuckDBPyConnection, klines_root: Path, ds: RawDataset
) -> None:
    write_batch(klines_root, B1, klines(0))
    bad = klines(3)
    bad["ETHUSDT"].append(bad["ETHUSDT"][0])
    write_batch(klines_root, B2, bad)
    write_batch(klines_root, B3, klines(6))

    loaded, problems = load_pending(con, ds)

    assert loaded == [B1]
    assert problems == [f"{B2}: ETHUSDT: 1 duplicate keys, manifest 0"]
    assert load_raw.loaded_batches(con, ds) == {B1}  # B3 waits for a human


def test_second_run_has_nothing_to_load(
    con: duckdb.DuckDBPyConnection, klines_root: Path, ds: RawDataset
) -> None:
    write_batch(klines_root, B1, klines(0))
    write_batch(klines_root, B2, klines(3))
    assert load_pending(con, ds) == ([B1, B2], [])
    assert load_pending(con, ds) == ([], [])
    assert count(con, ds.table) == 2 * 2 * ROWS


# --- main -----------------------------------------------------------------------------------


@pytest.fixture
def landing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    """Every dataset's landing zone and the warehouse file moved under tmp_path."""
    roots = {d.name: tmp_path / "raw" / d.name for d in DATASETS}
    datasets = tuple(dataclasses.replace(d, landing_dir=roots[d.name]) for d in DATASETS)
    monkeypatch.setattr(load_raw, "DATASETS", datasets)
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "warehouse.duckdb"))
    return roots


def funding(batch_hour: int) -> dict[str, list[Any]]:
    return {s: [make_funding(s, START_MS + batch_hour * HOUR_MS)] for s in SYMBOLS}


def test_main_without_any_batch_fails_before_opening_the_warehouse(
    landing: dict[str, Path], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert load_raw.main([]) == 1
    assert "No complete batch to load" in capsys.readouterr().out
    assert not (tmp_path / "warehouse.duckdb").exists()


def test_main_loads_every_dataset_and_a_rerun_has_nothing_to_load(
    landing: dict[str, Path], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_batch(landing["klines"], B1, klines(0))
    write_batch(landing["klines"], B2, klines(3))
    write_batch(landing["funding_rate"], B1, funding(0))

    assert load_raw.main([]) == 0
    out = capsys.readouterr().out
    assert f"klines loaded and reconciled 2 batches ({B1} .. {B2})" in out
    assert f"funding_rate loaded and reconciled 1 batches ({B1})" in out
    assert "open_interest: no complete batch" in out

    assert load_raw.main([]) == 0
    assert "OK: klines nothing to load; funding_rate nothing to load" in capsys.readouterr().out
    with connect(tmp_path / "warehouse.duckdb", read_only=True) as wh:
        assert count(wh, KLINES.table) == 2 * 2 * ROWS


def test_main_until_leaves_newer_batches_for_later(
    landing: dict[str, Path], tmp_path: Path
) -> None:
    write_batch(landing["klines"], B1, klines(0))
    write_batch(landing["klines"], B2, klines(3))
    assert load_raw.main(["--until", B1]) == 0
    with connect(tmp_path / "warehouse.duckdb", read_only=True) as wh:
        assert load_raw.loaded_batches(wh, KLINES) == {B1}


def test_main_a_batch_that_does_not_reconcile_fails_but_other_datasets_load(
    landing: dict[str, Path], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bad = klines(0)
    bad["BTCUSDT"].append(bad["BTCUSDT"][0])
    write_batch(landing["klines"], B1, bad)
    write_batch(landing["funding_rate"], B1, funding(0))

    assert load_raw.main([]) == 1
    out = capsys.readouterr().out
    assert f"klines: {B1}: BTCUSDT: 1 duplicate keys, manifest 0" in out
    with connect(tmp_path / "warehouse.duckdb", read_only=True) as wh:
        assert count(wh, FUNDING_RATE.table) == 2
        assert count(wh, KLINES.table) == 0


SKIP = 99  # what the Airflow DAG asks for; BashOperator reports it as skipped


def test_main_with_nothing_to_load_ends_with_the_code_asked_for(
    landing: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    write_batch(landing["klines"], B1, klines(0))
    assert load_raw.main(["--nothing-new-exit-code", str(SKIP)]) == 0  # a batch was loaded
    assert load_raw.main(["--nothing-new-exit-code", str(SKIP)]) == SKIP
    assert "OK: klines nothing to load" in capsys.readouterr().out


def test_main_one_new_batch_in_any_dataset_is_a_success(landing: dict[str, Path]) -> None:
    write_batch(landing["klines"], B1, klines(0))
    write_batch(landing["funding_rate"], B1, funding(0))
    assert load_raw.main([]) == 0
    # New klines only: the datasets after it (funding first) have nothing new this time.
    write_batch(landing["klines"], B2, klines(3))
    assert load_raw.main(["--nothing-new-exit-code", str(SKIP)]) == 0


def test_main_a_batch_that_does_not_reconcile_is_a_failure_not_nothing_new(
    landing: dict[str, Path],
) -> None:
    bad = klines(0)
    bad["BTCUSDT"].append(bad["BTCUSDT"][0])
    write_batch(landing["klines"], B1, bad)
    assert load_raw.main(["--nothing-new-exit-code", str(SKIP)]) == 1


def test_main_refuses_an_exit_code_outside_a_byte_before_opening_the_warehouse(
    landing: dict[str, Path], tmp_path: Path
) -> None:
    write_batch(landing["klines"], B1, klines(0))
    with pytest.raises(SystemExit) as exc:
        load_raw.main(["--nothing-new-exit-code", "256"])
    assert exc.value.code == 2
    assert not (tmp_path / "warehouse.duckdb").exists()


def test_main_refuses_an_until_that_is_not_a_batch_id(landing: dict[str, Path]) -> None:
    with pytest.raises(SystemExit) as exc:
        load_raw.main(["--until", "2026-11-25"])
    assert exc.value.code == 2


# --- the warehouse connection ---------------------------------------------------------------


def test_warehouse_sessions_work_in_utc(tmp_path: Path) -> None:
    with connect(tmp_path / "w.duckdb") as wh:
        row = wh.execute("SELECT current_setting('TimeZone')").fetchone()
    assert row == ("UTC",)


def test_warehouse_directory_must_exist(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Directory of the warehouse file"):
        connect(tmp_path / "nope" / "w.duckdb")
