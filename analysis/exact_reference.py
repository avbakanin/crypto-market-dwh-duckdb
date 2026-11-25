"""Exact references for metrics where DuckDB and the Snowflake baseline disagree.

    python -m analysis.exact_reference [--database PATH] [--output CSV]

Both engines compute some statistics in floating point, so a metric can differ beyond the
tolerance without either model being wrong in SQL - and the baseline itself can be the one
that is off. This module recomputes such metrics from the staging candles in Python
Decimal (50 significant digits), with the definitions of the dbt models, independently of
either engine. analysis/compare_baseline.py accepts a metric that differs from the baseline
only when DuckDB is within tolerance of this reference (and the baseline is not).

Metrics:
- int_binance__hourly_returns sum_return_zscore: the 1h return (step exactly one hour,
  rounded to 12 places as both warehouses store it) against the mean and the sample
  standard deviation of the returns in the return_zscore_window_hours rows before it, with
  at least return_zscore_min_hours returns there.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from collections.abc import Sequence
from decimal import ROUND_HALF_UP, Decimal, localcontext
from pathlib import Path

from ingestion.settings import PROJECT_ROOT, warehouse_path
from ingestion.warehouse import connect

REFERENCES_CSV = PROJECT_ROOT / "docs" / "baseline" / "exact_references.csv"
HOUR_MS = 3_600_000
RETURN_SCALE = Decimal("1e-12")  # decimal(38, 12)
# dbt vars return_zscore_window_hours / return_zscore_min_hours (a test keeps them equal).
ZSCORE_WINDOW = 720
ZSCORE_MIN = 168
DIGITS = 50
SOURCE = "python -m analysis.exact_reference (Decimal, 50 digits)"

Key = tuple[str, str, str, str]  # layer, relation, grain, metric


def hourly_returns(candles: Sequence[tuple[int, Decimal]]) -> list[Decimal | None]:
    """close / previous close - 1 per (open_time_ms, close) row, ordered by time; None for
    the first row and across a hole in the grid. Rounded half up to 12 places."""
    returns: list[Decimal | None] = []
    previous: tuple[int, Decimal] | None = None
    for open_ms, close in candles:
        if previous is not None and open_ms - previous[0] == HOUR_MS:
            returns.append((close / previous[1] - 1).quantize(RETURN_SCALE, ROUND_HALF_UP))
        else:
            returns.append(None)
        previous = (open_ms, close)
    return returns


def return_zscore_sum(
    returns: Sequence[Decimal | None], window: int = ZSCORE_WINDOW, minimum: int = ZSCORE_MIN
) -> Decimal:
    """Sum of the z-scores of one symbol's returns. The baseline of row i is rows
    i - window .. i - 1 (rows, not hours; NULL returns do not count); a z-score needs at
    least `minimum` returns there and a positive variance."""
    total = Decimal(0)
    s1 = s2 = Decimal(0)
    n = 0
    for i, r in enumerate(returns):
        if r is not None and n >= minimum:
            mean = s1 / n
            variance = (s2 - s1 * s1 / n) / (n - 1)
            if variance > 0:
                total += (r - mean) / variance.sqrt()
        if r is not None:
            s1, s2, n = s1 + r, s2 + r * r, n + 1
        if i >= window and (old := returns[i - window]) is not None:
            s1, s2, n = s1 - old, s2 - old * old, n - 1
    return total


def references(
    database: Path, window: int = ZSCORE_WINDOW, minimum: int = ZSCORE_MIN
) -> dict[Key, Decimal]:
    with connect(database, read_only=True) as con:
        rows = con.execute(
            "select symbol, open_time_ms, close_price::varchar"
            " from staging.stg_binance__klines_1h order by symbol, open_time_ms"
        ).fetchall()
    candles: dict[str, list[tuple[int, Decimal]]] = defaultdict(list)
    for symbol, open_ms, close in rows:
        candles[symbol].append((open_ms, Decimal(close)))
    with localcontext() as ctx:
        ctx.prec = DIGITS
        zscores = sum(
            (
                return_zscore_sum(hourly_returns(series), window, minimum)
                for series in candles.values()
            ),
            Decimal(0),
        )
    return {("intermediate", "int_binance__hourly_returns", "all", "sum_return_zscore"): zscores}


def write_references(path: Path, values: dict[Key, Decimal]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        out = csv.writer(f, lineterminator="\n")
        out.writerow(["layer", "relation", "grain", "metric", "reference", "source"])
        for key, value in sorted(values.items()):
            out.writerow([*key, f"{value:.15f}", SOURCE])


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Exact references of disputed metrics")
    parser.add_argument("--database", type=Path, help="default: DUCKDB_PATH")
    parser.add_argument("--output", type=Path, default=REFERENCES_CSV)
    args = parser.parse_args(argv)
    database = args.database.resolve() if args.database else warehouse_path()
    values = references(database)
    write_references(args.output, values)
    for key, value in sorted(values.items()):
        print(f"{'.'.join(key)}: {value:.15f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
