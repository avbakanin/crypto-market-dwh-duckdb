"""Tests for the exact (Decimal) references of disputed baseline metrics."""

from __future__ import annotations

import csv
from decimal import Decimal, localcontext
from pathlib import Path

import pytest
import yaml

from analysis import exact_reference as er
from ingestion.settings import PROJECT_ROOT
from ingestion.warehouse import connect

HOUR_MS = 3_600_000
T0 = 1_735_689_600_000  # 2025-01-01 00:00 UTC


def naive_zscore_sum(returns: list[Decimal | None], window: int, minimum: int) -> Decimal:
    """The definition, spelled out: the baseline is the `window` rows before, NULLs skipped."""
    total = Decimal(0)
    for i, r in enumerate(returns):
        if r is None:
            continue
        base = [x for x in returns[max(0, i - window) : i] if x is not None]
        if len(base) < minimum:
            continue
        mean = sum(base, Decimal(0)) / len(base)
        variance = sum(((x - mean) ** 2 for x in base), Decimal(0)) / (len(base) - 1)
        if variance > 0:
            total += (r - mean) / variance.sqrt()
    return total


def d(*values: str | None) -> list[Decimal | None]:
    return [None if v is None else Decimal(v) for v in values]


def test_returns_need_a_one_hour_step_and_keep_twelve_places() -> None:
    candles = [
        (T0, Decimal(100)),
        (T0 + HOUR_MS, Decimal(98)),  # exactly -2 %
        (T0 + 2 * HOUR_MS, Decimal(294)),
        (T0 + 4 * HOUR_MS, Decimal(300)),  # 2 hours after: no return across the hole
        (T0 + 5 * HOUR_MS, Decimal(400)),  # +1/3, rounded half up to 12 places
    ]
    assert er.hourly_returns(candles) == d(None, "-0.02", "2", None, "0.333333333333")


@pytest.mark.parametrize("window", [3, 4])
def test_rolling_zscore_sum_equals_the_definition(window: int) -> None:
    returns = d(None, "0.01", "-0.02", "0.03", None, "0.05", "-0.01", "0.02", "0.04", "-0.03")
    with localcontext() as ctx:
        ctx.prec = 50
        assert er.return_zscore_sum(returns, window, 2) == pytest.approx(
            naive_zscore_sum(returns, window, 2), abs=Decimal("1e-40")
        )


def test_the_baseline_excludes_the_current_hour() -> None:
    # Row 3 against rows 0-2 only: (3 - 2) / 1 = 1. With row 3 in its own baseline
    # (mean 2.5, sd 1.29) it would be 0.39.
    assert er.return_zscore_sum(d("1", "2", "3", "3"), 3, 3) == 1


def test_a_zscore_needs_the_minimum_number_of_returns() -> None:
    # Row 2 has 2 returns before it (z = (3 - 1.5) / 0.707 = 2.1213...).
    assert er.return_zscore_sum(d("1", "2", "3"), 3, 3) == 0
    assert er.return_zscore_sum(d("1", "2", "3"), 3, 2) == pytest.approx(
        Decimal("2.1213203435596424"), abs=Decimal("1e-15")
    )


def test_no_zscore_without_variance() -> None:
    assert er.return_zscore_sum(d("1", "1", "1", "5"), 3, 2) == 0


def test_window_and_minimum_match_the_dbt_vars() -> None:
    project = yaml.safe_load((PROJECT_ROOT / "dbt" / "dbt_project.yml").read_text("utf-8"))
    assert er.ZSCORE_WINDOW == project["vars"]["return_zscore_window_hours"]
    assert er.ZSCORE_MIN == project["vars"]["return_zscore_min_hours"]


CLOSES = ["100", "101", "99", "102", "100", "103", "98", "104"]
KEY = ("intermediate", "int_binance__hourly_returns", "all", "sum_return_zscore")


@pytest.fixture
def staging(tmp_path: Path) -> Path:
    """Staging candles of two symbols with the same closes; BBB starts a day later."""
    database = tmp_path / "w.duckdb"
    with connect(database) as con:
        con.execute("create schema staging")
        con.execute(
            "create table staging.stg_binance__klines_1h"
            " (symbol varchar, open_time_ms bigint, close_price decimal(38, 8))"
        )
        for symbol, start in (("AAAUSDT", T0), ("BBBUSDT", T0 + 24 * HOUR_MS)):
            for i, close in enumerate(CLOSES):
                con.execute(
                    "insert into staging.stg_binance__klines_1h values (?, ?, ?)",
                    [symbol, start + i * HOUR_MS, close],
                )
    return database


def test_references_sum_the_zscores_of_every_symbol(staging: Path) -> None:
    with localcontext() as ctx:
        ctx.prec = 50
        one = naive_zscore_sum(
            er.hourly_returns([(T0 + i * HOUR_MS, Decimal(c)) for i, c in enumerate(CLOSES)]),
            3,
            2,
        )
        both = 2 * one
    assert one != 0
    assert er.references(staging, 3, 2) == {KEY: pytest.approx(both, abs=Decimal("1e-40"))}


def test_main_writes_the_reference_csv(staging: Path, tmp_path: Path) -> None:
    output = tmp_path / "refs.csv"
    assert er.main(["--database", str(staging), "--output", str(output)]) == 0
    rows = list(csv.DictReader(output.open(encoding="utf-8", newline="")))
    assert [(r["layer"], r["relation"], r["grain"], r["metric"]) for r in rows] == [KEY]
    # 7 returns per symbol, far below the 168 the dbt vars require: no z-score at all.
    assert rows[0]["reference"] == "0.000000000000000"
    assert rows[0]["source"] == er.SOURCE
