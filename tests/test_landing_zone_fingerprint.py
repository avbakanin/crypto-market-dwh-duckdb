"""Tests for the RAW fingerprint from the landing zone: what the loader would load, compared."""

from __future__ import annotations

import csv
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from analysis import landing_zone_fingerprint as lzf
from ingestion import datasets
from ingestion.batches import write_manifest
from tests.factories import START_MS, make_funding, make_klines

HOUR = 3_600_000
B1, B2, B3 = "20250101T010000Z", "20250101T020000Z", "20250101T030000Z"


def write_batch(
    root: Path, batch_id: str, files: dict[str, list[Any]], complete: bool = True
) -> Path:
    """A batch as the extracts write it: one JSON file per symbol, the manifest last."""
    batch_dir = root / batch_id
    batch_dir.mkdir(parents=True)
    for symbol, records in files.items():
        (batch_dir / f"{symbol}.json").write_text(json.dumps(records), encoding="utf-8")
    if complete:
        stats = {s: {"rows": len(r), "duplicates": 0, "gaps": 0} for s, r in files.items()}
        write_manifest(
            batch_dir,
            {"batch_id": batch_id, "as_of_utc": "x", "interval": "1h", "symbols": stats},
        )
    return batch_dir


@pytest.fixture
def klines_root(tmp_path: Path) -> Path:
    """Two loaded batches overlapping by one candle, a newer one, and an incomplete one."""
    root = tmp_path / "klines_1h"
    write_batch(root, B1, {"BTCUSDT": make_klines(3), "ETHUSDT": make_klines(2)})
    write_batch(root, B2, {"BTCUSDT": make_klines(2, START_MS + 2 * HOUR)})
    write_batch(root, B3, {"BTCUSDT": make_klines(1, START_MS + 9 * HOUR)})
    write_batch(root, "20250101T040000Z", {"BTCUSDT": make_klines(5)}, complete=False)
    return root


def test_overlap_counts_as_rows_but_one_key(klines_root: Path) -> None:
    metrics = lzf.fingerprint(klines_root, lzf.FIELDS["klines"], until=B2)
    assert metrics["rows"] == "7"  # 3 + 2 + 2: RAW keeps both versions of the overlap
    assert metrics["distinct_keys"] == "6"  # BTC 00:00-03:00, ETH 00:00-01:00
    assert metrics["symbols"] == "2"
    assert (metrics["batches"], metrics["first_batch_id"], metrics["last_batch_id"]) == (
        "2",
        B1,
        B2,
    )
    keys = [START_MS + h * HOUR for h in (0, 1, 2, 0, 1, 2, 3)]
    assert metrics["min_key_ms"] == str(START_MS)
    assert metrics["max_key_ms"] == str(START_MS + 3 * HOUR)
    assert metrics["sum_key_ms"] == str(sum(keys))


def test_until_is_inclusive_and_incomplete_batches_are_not_loaded(klines_root: Path) -> None:
    assert lzf.fingerprint(klines_root, lzf.FIELDS["klines"], until=B3)["rows"] == "8"
    assert lzf.fingerprint(klines_root, lzf.FIELDS["klines"], until=None)["rows"] == "8"


def test_files_outside_the_manifest_are_not_loaded(tmp_path: Path) -> None:
    batch_dir = write_batch(tmp_path, B1, {"BTCUSDT": make_klines(2)})
    (batch_dir / "XRPUSDT.json").write_text(json.dumps(make_klines(4)), encoding="utf-8")
    assert lzf.fingerprint(tmp_path, lzf.FIELDS["klines"], until=None)["rows"] == "2"


def test_values_are_summed_exactly(tmp_path: Path) -> None:
    candles = make_klines(3)
    for candle, close in zip(candles, ("0.10000000", "0.20000000", "0.00000001"), strict=True):
        candle[4] = close
    write_batch(tmp_path, B1, {"BTCUSDT": candles})
    metrics = lzf.fingerprint(tmp_path, lzf.FIELDS["klines"], until=None)
    assert metrics["sum_close"] == "0.30000001"  # float would give 0.30000001000000004


def test_funding_key_is_the_hour_of_the_settlement(tmp_path: Path) -> None:
    on_hour, late = make_funding("BTCUSDT", START_MS), make_funding("BTCUSDT", START_MS)
    late["fundingTime"] = START_MS + 26  # the platform's millisecond jitter
    write_batch(tmp_path, B1, {"BTCUSDT": [on_hour]})
    write_batch(tmp_path, B2, {"BTCUSDT": [late]})
    metrics = lzf.fingerprint(tmp_path, lzf.FIELDS["funding_rate"], until=None)
    assert (metrics["rows"], metrics["distinct_keys"]) == ("2", "1")
    assert metrics["sum_key_ms"] == str(2 * START_MS)


def test_no_batch_up_to_until_is_an_error(klines_root: Path) -> None:
    with pytest.raises(ValueError, match="no complete batch"):
        lzf.fingerprint(klines_root, lzf.FIELDS["klines"], until="20240101T000000Z")


def test_every_dataset_has_fields_and_the_baseline_relation_name() -> None:
    assert set(lzf.FIELDS) == {d.name for d in datasets.BASELINE_DATASETS}
    names = {lzf.relation_name(d.table) for d in datasets.BASELINE_DATASETS}
    assert "binance_spot_klines_1h" in names and "binance_futures_funding_rate" in names


@pytest.mark.parametrize(
    ("ours", "theirs", "same"),
    [
        ("0.1", "0.10000000", True),
        ("204838", "204838", True),
        ("204838", "204839", False),
        (B1, B1, True),
        (B1, B2, False),
    ],
)
def test_numbers_compare_as_numbers_and_ids_as_text(ours: str, theirs: str, same: bool) -> None:
    assert lzf.same_value(ours, theirs) is same


def write_baseline(path: Path, rows: list[tuple[str, str, str, str, str, str]]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as f:
        out = csv.writer(f, lineterminator="\n")
        out.writerow(["layer", "relation", "grain", "metric", "compare", "value"])
        out.writerows(rows)
    return path


def test_differences_name_wrong_and_missing_metrics_and_skip_inexact(tmp_path: Path) -> None:
    baseline = lzf.read_baseline(
        write_baseline(
            tmp_path / "b.csv",
            [
                ("raw", "t", "all", "rows", "exact", "7"),
                ("raw", "t", "all", "symbols", "exact", "3"),
                ("raw", "t", "all", "batches", "exact", "2"),
                ("raw", "t", "all", "content_hash", "snowflake_only", "123"),
                ("raw", "t", "BTCUSDT", "rows", "exact", "5"),
                ("staging", "t", "all", "rows", "exact", "1"),
            ],
        )
    )
    problems = lzf.differences({"t": {"rows": "7", "symbols": "2"}}, baseline)
    assert problems == [
        "t.batches: not computed (baseline 2)",
        "t.symbols: landing zone 2, baseline 3",
    ]


@pytest.fixture
def one_dataset(monkeypatch: pytest.MonkeyPatch, klines_root: Path) -> None:
    monkeypatch.setattr(lzf, "DATASETS", (replace(datasets.KLINES, landing_dir=klines_root),))


def baseline_of(path: Path, overrides: dict[str, str]) -> Path:
    metrics = lzf.fingerprint(lzf.DATASETS[0].landing_dir, lzf.FIELDS["klines"], until=B2)
    metrics |= overrides
    rows = [("raw", "binance_spot_klines_1h", "all", m, "exact", v) for m, v in metrics.items()]
    return write_baseline(path, rows)


@pytest.mark.usefixtures("one_dataset")
def test_main_passes_when_the_landing_zone_rebuilds_the_baseline(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline = baseline_of(tmp_path / "b.csv", {})
    assert lzf.main(["--until", B2, "--compare", str(baseline)]) == 0
    out, err = capsys.readouterr()
    assert out.splitlines()[1] == "raw,binance_spot_klines_1h,all,rows,exact,7"
    assert "OK: 10 raw metrics equal" in err


@pytest.mark.usefixtures("one_dataset")
def test_main_fails_on_a_difference(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    baseline = baseline_of(tmp_path / "b.csv", {"sum_close": "1"})
    assert lzf.main(["--until", B2, "--compare", str(baseline)]) == 1
    err = capsys.readouterr().err
    assert "DIFF binance_spot_klines_1h.sum_close" in err and "FAIL: 1 raw metrics" in err


def test_main_rejects_a_malformed_until() -> None:
    with pytest.raises(SystemExit) as exit_info:
        lzf.main(["--until", "2026-11-25"])
    assert exit_info.value.code == 2
