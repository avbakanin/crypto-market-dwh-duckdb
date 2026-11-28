"""Event study: what happened after a condition, net of costs, against the same days' base.

Run from repo root (the pipeline must not be writing the warehouse):
    python -m research.event_study "funding_z_30d > 2 and oi_chg_7d > 0.2" --side short
    python -m research.event_study "rank_ret_7d < -0.4" --side long --max-rank 50 --min-history 180

The condition is a pandas query over int_um__features_daily (decision date D, features of
days <= D-1; docs/features_and_labels.md) plus rank_<feature> (the day's centred percent
rank, -0.5..0.5) and market_funding_1d. For every horizon (1, 3, 7, 14 days) it prints:
events, distinct days, mean net return of the side (fct_um_forward_returns: entry D 01:00
UTC, taker fees, slippage, funding), its 95 % interval and p by a block bootstrap over days,
hit rate, the excess over the day's average member on the same side (base), MAE, and the
share of events a 3x / 5x position would have survived. Then the mean by year.

Every new condition is recorded in the hypothesis ledger (research/ledger.py); p_adj is
p times the number of hypotheses tried so far. A tool for looking, not a signal: a result
becomes a signal only through a registered version and the forward test (signals/).
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd

from research import data, ledger, stats
from research.config import MAINTENANCE_MARGIN, SEED

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HORIZONS = (1, 3, 7, 14)
LEVERAGES = (3, 5)
SAMPLES = 5_000


def read_panel(warehouse: Path) -> pd.DataFrame:
    """Every feature column, the day's ranks, and long/short net returns and MAE per horizon."""
    import duckdb

    with duckdb.connect(str(warehouse), read_only=True) as con:
        con.execute("set TimeZone = 'UTC'")
        features = con.execute("select * from staging.int_um__features_daily").df()
        labels = con.execute(
            "select decision_date, symbol, horizon_days, net_return_long::double as nl, "
            "net_return_short::double as ns, mae_long::double as mael, "
            "mae_short::double as maes from marts.fct_um_forward_returns "
            f"where horizon_days in ({', '.join(map(str, HORIZONS))})"
        ).df()
    extra = tuple(c for c in features.columns if c not in ("decision_date", "symbol"))
    panel = data.feature_panel(features, extra=extra)
    return attach_labels(panel, labels)


def attach_labels(panel: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    wide = labels.copy()
    wide["decision_date"] = pd.to_datetime(wide["decision_date"])
    wide = wide.pivot_table(
        index=["decision_date", "symbol"],
        columns="horizon_days",
        values=["nl", "ns", "mael", "maes"],
    )
    wide.columns = [f"{name}_{h}" for name, h in wide.columns]
    return panel.merge(wide.reset_index(), on=["decision_date", "symbol"], how="left")


def survives(mae: pd.Series, leverage: int) -> pd.Series:
    """MAE (a negative return) stays above the liquidation distance of an isolated position."""
    return mae > -(1 / leverage - MAINTENANCE_MARGIN)


def study(
    panel: pd.DataFrame,
    condition: str,
    side: str,
    horizons: Sequence[int] = HORIZONS,
    samples: int = SAMPLES,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per-horizon summary and per-year means of the events matching `condition`."""
    net, mae = ("nl", "mael") if side == "long" else ("ns", "maes")
    panel = panel.copy()
    for h in horizons:
        panel[f"base_{h}"] = panel.groupby("decision_date")[f"{net}_{h}"].transform("mean")
    events = panel.query(condition, engine="python")
    rows, years = [], []
    for h in horizons:
        done = events.dropna(subset=[f"{net}_{h}"])
        if done.empty:
            rows.append({"horizon": h, "events": 0})
            continue
        returns = cast(pd.Series, done[f"{net}_{h}"])
        adverse = cast(pd.Series, done[f"{mae}_{h}"])
        trades = pd.DataFrame(
            {
                "decision_date": done["decision_date"],
                "net": returns,
                "excess": returns - done[f"base_{h}"],
            }
        )
        start = stats.as_date(trades["decision_date"].min())
        end = date.fromordinal(stats.as_date(trades["decision_date"].max()).toordinal() + 1)
        block = max(7, h)
        mean, low, high, p = stats.block_bootstrap_mean(
            stats.daily_series(trades, "net", start, end), block, samples, SEED
        )
        ex, ex_low, ex_high, ex_p = stats.block_bootstrap_mean(
            stats.daily_series(trades, "excess", start, end), block, samples, SEED
        )
        row = {
            "horizon": h,
            "events": len(done),
            "days": done["decision_date"].nunique(),
            "mean_net": mean,
            "ci_low": low,
            "ci_high": high,
            "p": p,
            "hit_rate": float((returns > 0).mean()),
            "excess": ex,
            "excess_ci_low": ex_low,
            "excess_ci_high": ex_high,
            "excess_p": ex_p,
            "mae_median": float(adverse.median()),
            "mae_p10": float(adverse.quantile(0.10)),
        }
        for lev in LEVERAGES:
            row[f"survive_{lev}x"] = float(survives(adverse, lev).mean())
        rows.append(row)
        by_year = done.groupby(done["decision_date"].dt.year)[f"{net}_{h}"]
        for year, values in by_year:
            years.append(
                {"horizon": h, "year": year, "events": len(values), "mean_net": values.mean()}
            )
    return pd.DataFrame(rows), pd.DataFrame(years)


def hypothesis_name(condition: str, side: str, filters: str) -> str:
    return f"event:{side}:{re.sub(r'\s+', ' ', condition.strip())}{filters}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Event study over the feature panel.")
    parser.add_argument(
        "condition", help="pandas query over the features, e.g. 'funding_z_30d > 2'"
    )
    parser.add_argument("--side", choices=["long", "short"], required=True)
    parser.add_argument("--start", type=date.fromisoformat, help="first decision date")
    parser.add_argument("--end", type=date.fromisoformat, help="last decision date (exclusive)")
    parser.add_argument("--max-rank", type=int, help="turnover rank at most N (liquidity)")
    parser.add_argument("--min-history", type=int, help="at least N days of history")
    parser.add_argument(
        "--warehouse", type=Path, default=PROJECT_ROOT / "warehouse" / "crypto_dwh.duckdb"
    )
    args = parser.parse_args(argv)

    panel = read_panel(args.warehouse)
    filters = ""
    if args.start:
        panel = panel.loc[panel["decision_date"] >= pd.Timestamp(args.start)]
        filters += f" from {args.start}"
    if args.end:
        panel = panel.loc[panel["decision_date"] < pd.Timestamp(args.end)]
        filters += f" to {args.end}"
    if args.max_rank:
        panel = panel.loc[panel["turnover_rank"] <= args.max_rank]
        filters += f" rank<={args.max_rank}"
    if args.min_history:
        panel = panel.loc[panel["history_days"] >= args.min_history]
        filters += f" history>={args.min_history}"

    try:
        summary, years = study(panel, args.condition, args.side)
    except Exception as exc:  # noqa: BLE001 - a bad query is a user error, not a crash
        parser.error(f"condition failed: {type(exc).__name__}: {exc}")

    ledger.backfill()
    name = hypothesis_name(args.condition, args.side, filters)
    if name not in ledger.tested():
        ledger.record([name], "event_study", datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))
    attempts = len(ledger.tested())
    if "p" in summary:
        summary["p_adj"] = [
            ledger.adjusted_p(p, attempts) if not np.isnan(p) else np.nan for p in summary["p"]
        ]

    first, last = panel["decision_date"].min().date(), panel["decision_date"].max().date()
    print(f"{name}\nPanel {first} .. {last}; hypotheses in the ledger: {attempts}\n")
    with pd.option_context("display.width", 200, "display.max_columns", 30, "display.precision", 4):
        print(summary.to_string(index=False))
        if not years.empty:
            print("\nMean net by year:")
            print(years.pivot(index="year", columns="horizon", values="mean_net").to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
