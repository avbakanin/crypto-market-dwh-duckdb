"""Daily market context for Telegram: extremes among the universe members. Not signals.

Run from repo root: python -m signals.screener [--no-telegram] [--nothing-new-exit-code 99]

Once a day, as soon as the features of today (decision date D, data of days <= D-1) are
built: the members that meet each rule, top ROWS_PER_RULE by its sort column, and a market
line. The message is saved to data/signals/screener/<D>.txt, which also marks the day as
sent (a later run exits with the nothing-new code). No rule here is a tested edge: each
condition is a pandas query that `python -m research.event_study "<query>" --side ...`
takes as it is, to see what followed it in the past.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = PROJECT_ROOT / "warehouse" / "crypto_dwh.duckdb"
OUT_DIR = PROJECT_ROOT / "data" / "signals" / "screener"
ROWS_PER_RULE = 5


@dataclass(frozen=True)
class Rule:
    title: str
    query: str  # over int_um__features_daily plus rank_<feature> (research.data)
    sort: str
    descending: bool = True


RULES = (
    Rule("Funding hot (longs pay)", "funding_z_30d >= 3", "funding_z_30d"),
    Rule("Funding cold (shorts pay)", "funding_z_30d <= -3", "funding_z_30d", descending=False),
    Rule("Open interest spike", "oi_chg_1d >= 0.25 or oi_chg_7d >= 0.5", "oi_chg_7d"),
    Rule(
        "Short squeeze setup (shorts pile in and pay)",
        "funding_1d < 0 and funding_z_30d <= -2 and oi_chg_7d >= 0.2",
        "oi_chg_7d",
    ),
    Rule(
        "Long squeeze setup (longs pile in, price falls)",
        "funding_z_30d >= 2 and oi_chg_7d >= 0.2 and ret_7d <= 0",
        "oi_chg_7d",
    ),
    Rule(
        "Top traders long, crowd short",
        "rank_top_position_ls - rank_global_ls >= 0.7",
        "rank_top_position_ls",
    ),
    Rule(
        "Top traders short, crowd long",
        "rank_top_position_ls - rank_global_ls <= -0.7",
        "rank_top_position_ls",
        descending=False,
    ),
)


def read_today(warehouse: Path, day: date) -> pd.DataFrame:
    """D's member features with the day's ranks (empty until the build of D)."""
    import duckdb

    from research import data

    with duckdb.connect(str(warehouse), read_only=True) as con:
        con.execute("set TimeZone = 'UTC'")
        features = con.execute(
            "select * from staging.int_um__features_daily where decision_date = ?", [day]
        ).df()
    if features.empty:
        return features
    extra = tuple(c for c in features.columns if c not in ("decision_date", "symbol"))
    return data.feature_panel(features, extra=extra)


def fmt(value: object, spec: str) -> str:
    """A feature value, or n/a when it is missing (features start at different dates)."""
    return "n/a" if pd.isna(value) else format(float(value), spec)  # type: ignore[arg-type]


def pct(value: object) -> str:
    return fmt(value, "+.0%")


def describe(row: pd.Series) -> str:
    return (
        f"{row['symbol']} (rank {int(row['turnover_rank'])}): "
        f"funding z {fmt(row['funding_z_30d'], '+.1f')}, "
        f"OI 1d {pct(row['oi_chg_1d'])} 7d {pct(row['oi_chg_7d'])}, "
        f"ret 7d {pct(row['ret_7d'])}, top/global L/S "
        f"{fmt(row['top_position_ls'], '.2f')}/{fmt(row['global_ls'], '.2f')}"
    )


def matches(panel: pd.DataFrame, rule: Rule) -> pd.DataFrame:
    hits = panel.query(rule.query, engine="python")
    return hits.sort_values(rule.sort, ascending=not rule.descending).head(ROWS_PER_RULE)


def market_line(panel: pd.DataFrame) -> str:
    first = panel.iloc[0]
    trend = "above" if first["btc_close_to_sma50"] > 0 else "below"
    return (
        f"Market: BTC {trend} its 50-day SMA ({pct(first['btc_close_to_sma50'])}), "
        f"BTC 7d {pct(first['btc_ret_7d'])}, members above SMA20 "
        f"{fmt(first['breadth_above_sma20'], '.0%')}, median funding 1d "
        f"{fmt(panel['funding_1d'].median(), '+.4%')}"
    )


def message(day: date, panel: pd.DataFrame) -> str:
    lines = [
        f"🔎 CONTEXT, not a signal - market of {day} (data to {day - timedelta(days=1)}, "
        f"{len(panel)} members)",
        market_line(panel),
    ]
    for rule in RULES:
        hits = matches(panel, rule)
        if hits.empty:
            continue
        lines.append(f"\n{rule.title}:")
        lines += [f"  {describe(row)}" for _, row in hits.iterrows()]
    if len(lines) == 2:
        lines.append("No member meets a rule today.")
    lines.append("\nUntested rules; history of one: python -m research.event_study")
    return "\n".join(lines)


def utc_today() -> date:
    return datetime.now(UTC).date()


def main(argv: Sequence[str] | None = None, today: Callable[[], date] = utc_today) -> int:
    parser = argparse.ArgumentParser(description="Send today's market context to Telegram.")
    parser.add_argument("--warehouse", type=Path, default=WAREHOUSE)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--nothing-new-exit-code", type=int, default=0)
    args = parser.parse_args(argv)
    day = today()
    out = args.out_dir / f"{day}.txt"
    if out.exists():
        print(f"context of {day} is sent already")
        return args.nothing_new_exit_code
    panel = read_today(args.warehouse, day)
    if panel.empty:
        print(f"no features of {day} yet")
        return args.nothing_new_exit_code
    text = message(day, panel)
    print(text)
    if not args.no_telegram:
        from signals import notify

        notify.send(text)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out.write_text(text + "\n", encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
