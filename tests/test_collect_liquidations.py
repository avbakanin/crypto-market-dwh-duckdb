"""Tests for the liquidation collector: spool by receive hour, sealing into immutable hourly
batches (crash cases included), and a sealed batch reconciling in RAW."""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ingestion import collect_liquidations as cl
from ingestion.batches import MANIFEST_NAME, read_manifest
from ingestion.datasets import UM_LIQUIDATIONS
from ingestion.load_raw import ensure_raw_objects, load_pending

HOUR = int(datetime(2026, 11, 28, 8, tzinfo=UTC).timestamp() * 1000)
BATCH_ID = "20261128T090000Z"  # the hour's end


def message(symbol: str, trade_ms: int, side: str = "SELL", qty: str = "1.5") -> dict[str, Any]:
    return {
        "e": "forceOrder",
        "E": trade_ms + 7,
        "o": {"s": symbol, "S": side, "o": "LIMIT", "f": "IOC", "q": qty, "p": "100.0",
              "ap": "100.5", "X": "FILLED", "l": qty, "z": qty, "T": trade_ms},
    }  # fmt: skip


def write_spool(spool_dir: Path, hour_ms: int, entries: list[dict[str, Any]]) -> Path:
    spool = cl.Spool(spool_dir)
    for entry in entries:
        spool.write(entry)
    spool.close()
    path = spool_dir / cl.spool_name(hour_ms)
    assert path.is_file()
    return path


def hour_entries() -> list[dict[str, Any]]:
    return [
        {"r": HOUR + 1_000, "c": "open"},
        {"r": HOUR + 2_000, "m": message("BTCUSDT", HOUR + 1_990)},
        {"r": HOUR + 3_000, "m": message("ETHUSDT", HOUR + 2_990, side="BUY")},
        {"r": HOUR + 21_000, "c": "alive"},
        {"r": HOUR + 61_000, "m": message("BTCUSDT", HOUR + 60_990, qty="2")},
        {"r": HOUR + 62_000, "c": "close"},
    ]


def test_spool_files_follow_the_receive_hour(tmp_path: Path) -> None:
    spool = cl.Spool(tmp_path)
    spool.write({"r": HOUR + cl.HOUR_MS - 1, "c": "alive"})
    spool.write({"r": HOUR + cl.HOUR_MS, "c": "alive"})
    spool.close()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["20261128T08.jsonl", "20261128T09.jsonl"]
    assert cl.spool_hour(tmp_path / "20261128T09.jsonl") == HOUR + cl.HOUR_MS
    assert cl.spool_hour(tmp_path / "notes.txt") is None


def test_sealed_hour_is_a_batch_per_contract_plus_the_stream(tmp_path: Path) -> None:
    path = write_spool(tmp_path / "spool", HOUR, hour_entries())
    batch = cl.seal(path, tmp_path / "out")
    assert batch is not None and batch.name == BATCH_ID
    assert not path.exists()
    manifest = read_manifest(batch)
    assert manifest["interval"] == "event"
    assert manifest["symbols"] == {
        "BTCUSDT": {"rows": 2, "duplicates": 0, "first_ms": HOUR + 1_990, "last_ms": HOUR + 60_990},
        "ETHUSDT": {"rows": 1, "duplicates": 0, "first_ms": HOUR + 2_990, "last_ms": HOUR + 2_990},
        "STREAM": {"rows": 3, "duplicates": 0, "first_ms": HOUR + 1_000, "last_ms": HOUR + 62_000},
    }
    btc = json.loads((batch / "BTCUSDT.json").read_text(encoding="utf-8"))
    assert btc == [message("BTCUSDT", HOUR + 1_990), message("BTCUSDT", HOUR + 60_990, qty="2")]


def test_torn_last_line_after_a_crash_is_skipped(tmp_path: Path) -> None:
    path = write_spool(tmp_path / "spool", HOUR, hour_entries())
    with path.open("a", encoding="utf-8") as f:
        f.write('{"r": 17958672')  # the process died mid-line
    batch = cl.seal(path, tmp_path / "out")
    assert batch is not None
    assert sum(s["rows"] for s in read_manifest(batch)["symbols"].values()) == 6


def test_identical_messages_are_counted_as_duplicates(tmp_path: Path) -> None:
    same = message("BTCUSDT", HOUR + 5_000)
    path = write_spool(tmp_path / "spool", HOUR, [{"r": HOUR + 5_001, "m": same}] * 2)
    batch = cl.seal(path, tmp_path / "out")
    assert batch is not None
    assert read_manifest(batch)["symbols"]["BTCUSDT"]["duplicates"] == 1


def test_crash_after_the_manifest_only_removes_the_spool(tmp_path: Path) -> None:
    out = tmp_path / "out"
    path = write_spool(tmp_path / "spool", HOUR, hour_entries())
    first = cl.seal(path, out)
    assert first is not None
    before = (first / MANIFEST_NAME).read_bytes()
    path = write_spool(tmp_path / "spool", HOUR, hour_entries()[:2])  # spool left behind
    assert cl.seal(path, out) == first
    assert not path.exists()
    assert (first / MANIFEST_NAME).read_bytes() == before  # the batch is never rewritten


def test_batch_without_manifest_is_replaced(tmp_path: Path) -> None:
    out = tmp_path / "out"
    torn = out / BATCH_ID
    torn.mkdir(parents=True)
    (torn / "BTCUSDT.json").write_text("[", encoding="utf-8")  # crash while sealing
    path = write_spool(tmp_path / "spool", HOUR, hour_entries())
    batch = cl.seal(path, out)
    assert batch == torn
    assert set(read_manifest(torn)["symbols"]) == {"BTCUSDT", "ETHUSDT", "STREAM"}


def test_only_hours_past_the_grace_period_are_sealed(tmp_path: Path) -> None:
    spool_dir, out = tmp_path / "spool", tmp_path / "out"
    write_spool(spool_dir, HOUR, hour_entries())
    nxt = HOUR + cl.HOUR_MS
    write_spool(spool_dir, nxt, [{"r": nxt + 1_000, "c": "alive"}])
    assert cl.seal_finished(spool_dir, out, nxt + cl.SEAL_GRACE_MS - 1) == []
    assert [b.name for b in cl.seal_finished(spool_dir, out, nxt + cl.SEAL_GRACE_MS)] == [BATCH_ID]
    assert [p.name for p in spool_dir.iterdir()] == ["20261128T09.jsonl"]  # still being written


def test_sealed_batch_reconciles_in_raw(tmp_path: Path) -> None:
    out = tmp_path / "um_liquidations"
    same = message("SOLUSDT", HOUR + 9_000)
    entries = [*hour_entries(), {"r": HOUR + 9_001, "m": same}, {"r": HOUR + 9_002, "m": same}]
    cl.seal(write_spool(tmp_path / "spool", HOUR, entries), out)
    ds = dataclasses.replace(UM_LIQUIDATIONS, landing_dir=out)
    with duckdb.connect(config={"TimeZone": "UTC"}) as con:
        ensure_raw_objects(con)
        loaded, problems = load_pending(con, ds)
        assert (loaded, problems) == ([BATCH_ID], [])
        rows = con.execute(
            "SELECT symbol, count(*) FROM raw.binance_um_liquidations GROUP BY 1 ORDER BY 1"
        ).fetchall()
    assert rows == [("BTCUSDT", 2), ("ETHUSDT", 1), ("SOLUSDT", 2), ("STREAM", 3)]


def test_collector_refuses_a_landing_zone_it_does_not_own(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PIPELINE_MODE", "follow")
    assert cl.main() == 2
