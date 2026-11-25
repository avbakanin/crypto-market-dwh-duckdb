"""Tests for the comparison of the DuckDB warehouse with the Snowflake baseline."""

from __future__ import annotations

import csv
import dataclasses
from pathlib import Path

import pytest

from analysis import compare_baseline as cb
from ingestion import load_raw
from ingestion.datasets import DATASETS
from tests.factories import START_MS, make_funding, make_klines
from tests.test_load_raw import B1, B2, write_batch

HOUR_MS = 3_600_000


@pytest.mark.parametrize(
    ("ours", "theirs", "equal"),
    [
        ("0.1", "0.10000000", True),
        ("204838", "204838", True),
        ("5", "6", False),
        ("20261125T090706Z", "20261125T090706Z", True),
        ("20261125T090706Z", "20261124T000000Z", False),
    ],
)
def test_exact_compares_numbers_as_numbers_and_text_as_text(
    ours: str, theirs: str, equal: bool
) -> None:
    assert cb.same_value(ours, theirs) is equal


@pytest.mark.parametrize(
    ("ours", "equal"),
    [("1.2345", True), ("1.2350", True), ("1.2340", True), ("1.2351", False), ("1.2339", False)],
)
def test_tolerance_is_five_units_of_the_baselines_last_digit(ours: str, equal: bool) -> None:
    assert cb.within_tolerance(ours, "1.2345") is equal


def test_each_class_is_compared_its_own_way() -> None:
    baseline = {
        ("raw", "t", "all", "a"): ("exact", "10"),
        ("raw", "t", "all", "b"): ("exact", "10"),
        ("raw", "t", "all", "c"): ("exact", "10"),
        ("raw", "t", "all", "d"): ("tolerance", "1.000"),
        ("raw", "t", "all", "e"): ("snowflake_only", "123"),
    }
    ours = {
        ("raw", "t", "all", "a"): "10",
        ("raw", "t", "all", "b"): "11",
        ("raw", "t", "all", "d"): "1.004",
        ("raw", "t", "all", "e"): "999",
    }
    assert [o.status for o in cb.compare(baseline, ours)] == [
        "equal",
        "differs",
        "missing",
        "equal",
        "not compared",
    ]


Z: cb.Key = ("intermediate", "r", "all", "sum_return_zscore")


@pytest.mark.parametrize(
    ("ours", "baseline", "reference", "status"),
    [
        # The baseline is off, DuckDB is at the exact value (the real case).
        ("-403.465601", "-403.465608", "-403.465600880930617", "baseline error"),
        # DuckDB is off too: not rescued by the reference.
        ("-403.465620", "-403.465608", "-403.465600880930617", "differs"),
        # The baseline is within tolerance of the truth, so the difference is ours.
        ("1.0085", "1.000", "1.004", "differs"),
        # Without a reference a difference stays a difference.
        ("-403.465601", "-403.465608", None, "differs"),
        # Equal to the baseline: the reference is not looked at.
        ("-403.465606", "-403.465608", "0", "equal"),
    ],
)
def test_a_baseline_error_needs_duckdb_at_the_exact_value_and_the_baseline_off(
    ours: str, baseline: str, reference: str | None, status: str
) -> None:
    references = {} if reference is None else {Z: reference}
    (outcome,) = cb.compare({Z: ("tolerance", baseline)}, {Z: ours}, references)
    assert outcome.status == status


def test_tolerance_against_a_reference_counts_the_baselines_digits() -> None:
    # 5 units of the baseline's 6th decimal, not of the reference's 15th.
    assert cb.within_tolerance("-403.465601", "-403.465600880930617", digits_of="-403.465608")
    assert not cb.within_tolerance("-403.465601", "-403.465600880930617")


def test_engine_dependent_values_side_by_side() -> None:
    key = ("marts", "t", "all", "rows")
    (outcome,) = cb.compare({key: ("engine_dependent", "559335")}, {key: "558698"})
    assert outcome.status == "not compared"
    assert cb.side_by_side(outcome) == "DuckDB 558698, Snowflake 559335 (-0.11 %)"


def write_baseline(path: Path, rows: list[tuple[str, str, str, str, str, str]]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as f:
        out = csv.writer(f, lineterminator="\n")
        out.writerow(["layer", "relation", "grain", "metric", "compare", "value"])
        out.writerows(rows)
    return path


@pytest.fixture
def warehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A warehouse loaded by the loader: two klines batches of two symbols (B2 repeats one
    candle of B1), and funding a few ms after two hours."""
    roots = {d.name: tmp_path / "raw" / d.name for d in DATASETS}
    monkeypatch.setattr(
        load_raw,
        "DATASETS",
        tuple(dataclasses.replace(d, landing_dir=roots[d.name]) for d in DATASETS),
    )
    database = tmp_path / "w.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(database))
    for batch_id, first_hour, n in ((B1, 0, 3), (B2, 2, 2)):
        candles = make_klines(n, START_MS + first_hour * HOUR_MS)
        write_batch(roots["klines"], batch_id, {"BTCUSDT": candles, "ETHUSDT": candles})
    rates = [
        make_funding("BTCUSDT", START_MS + 3, "0.00010000"),
        make_funding("BTCUSDT", START_MS + HOUR_MS + 7, "-0.00002500"),
    ]
    write_batch(roots["funding_rate"], B1, {"BTCUSDT": rates})
    assert load_raw.main([]) == 0
    return database


def klines_metric(
    metric: str, value: str, cls: str = "exact"
) -> tuple[str, str, str, str, str, str]:
    return ("raw", "binance_spot_klines_1h", "all", metric, cls, value)


def test_raw_metrics_of_the_loaded_warehouse(warehouse: Path) -> None:
    ours = cb.duckdb_metrics(warehouse, ["raw"])
    klines = {k[3]: v for k, v in ours.items() if k[1] == "binance_spot_klines_1h"}
    assert klines == {
        "rows": "10",
        "symbols": "2",
        "batches": "2",
        "first_batch_id": B1,
        "last_batch_id": B2,
        "distinct_keys": "8",  # per symbol; the repeated candle counts once
        "min_key_ms": str(START_MS),
        "max_key_ms": str(START_MS + 3 * HOUR_MS),
        "sum_key_ms": str(2 * (5 * START_MS + (0 + 1 + 2 + 2 + 3) * HOUR_MS)),
        "sum_close": "1005.00000000",  # make_kline closes at 100.5
    }
    funding = {k[3]: v for k, v in ours.items() if k[1] == "binance_futures_funding_rate"}
    assert (funding["min_key_ms"], funding["max_key_ms"]) == (
        str(START_MS),
        str(START_MS + HOUR_MS),
    )
    assert funding["sum_key_ms"] == str(2 * START_MS + HOUR_MS)  # whole hours, not the ms
    assert funding["sum_funding_rate"] == "0.00007500"
    assert {k[1] for k in ours} == {d.table.split(".")[1] for d in DATASETS}


def test_main_passes_when_the_baseline_matches_and_reports(
    warehouse: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline = write_baseline(
        tmp_path / "b.csv",
        [
            klines_metric("rows", "10"),
            klines_metric("sum_close", "1005"),
            klines_metric("content_hash", "42", "snowflake_only"),
            ("staging", "x", "all", "rows", "exact", "1"),  # another layer: not asked for
        ],
    )
    report = tmp_path / "r.md"
    args = ["--layer", "raw", "--database", str(warehouse), "--baseline", str(baseline)]
    assert cb.main([*args, "--report", str(report)]) == 0
    assert "OK: every compared metric of raw equals the baseline" in capsys.readouterr().out
    text = report.read_text(encoding="utf-8")
    assert "| raw | exact | 2 | 2 | 0 | 0 | 0 | 0 |" in text
    assert "| raw | snowflake_only | 1 | 0 | 0 | 0 | 0 | 1 |" in text
    assert "None: every exact and tolerance metric" in text


def test_main_fails_on_a_difference_and_names_it(
    warehouse: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline = write_baseline(
        tmp_path / "b.csv", [klines_metric("rows", "11"), klines_metric("no_such_metric", "1")]
    )
    args = ["--layer", "raw", "--database", str(warehouse), "--baseline", str(baseline)]
    assert cb.main(args) == 1
    out = capsys.readouterr().out
    assert "DIFF raw.binance_spot_klines_1h.all.rows (exact): DuckDB 10, baseline 11" in out
    assert "DIFF raw.binance_spot_klines_1h.all.no_such_metric (exact): DuckDB None" in out


def test_main_reports_a_baseline_error_and_passes(
    warehouse: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline = write_baseline(
        tmp_path / "b.csv",
        [
            klines_metric("sum_close", "1006.0", "tolerance"),
            klines_metric("rows", "12", "engine_dependent"),
        ],
    )
    references = tmp_path / "refs.csv"
    with references.open("w", encoding="utf-8", newline="") as f:
        out = csv.writer(f, lineterminator="\n")
        out.writerow(["layer", "relation", "grain", "metric", "reference", "source"])
        out.writerow(["raw", "binance_spot_klines_1h", "all", "sum_close", "1005.0001", "test"])
    report = tmp_path / "r.md"
    args = ["--layer", "raw", "--database", str(warehouse), "--baseline", str(baseline)]
    assert cb.main([*args, "--references", str(references), "--report", str(report)]) == 0
    out_text = capsys.readouterr().out
    assert "BASELINE ERROR raw.binance_spot_klines_1h.all.sum_close: exact 1005.0001" in out_text
    text = report.read_text(encoding="utf-8")
    assert "| raw | tolerance | 1 | 0 | 1 | 0 | 0 | 0 |" in text
    assert "## Baseline errors" in text
    assert "`rows`: engine_dependent - DuckDB 10, Snowflake 12 (-16.67 %)" in text


def test_main_refuses_a_layer_without_a_query(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        cb.main(["--layer", "nope", "--database", str(tmp_path / "w.duckdb")])
    assert exc.value.code == 2
