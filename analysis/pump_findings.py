"""The pump findings, recomputed from the newest mart export (data/marts), not the warehouse.

    python -m analysis.pump_findings [--marts DIR] [--boot N]

Prints a Markdown report. Every number comes with the events and days behind it, and the
comparisons with a 95 % confidence interval from a bootstrap over event days. Days, not
events: pumps of one day move together (a market rally lifts ten coins in the same hour),
so resampling single events would treat them as independent and narrow the interval.

Comparison: each event against its own control hours - ordinary hours of the same coin,
measured by the same SQL (fct_pump_control_window) - as excess = event value - median of
its controls (fct_pump_event_excess). The next-hour check (mart_pump_signal_check) turns
the view around: how often an hour that looks like a pre-pump hour is followed by a pump.

Needs the dashboard's requirements (duckdb, pandas): pip install -r requirements-dashboard.txt
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from dashboard import data

MARTS_DIR = Path(__file__).resolve().parents[1] / "data" / "marts"
N_BOOT = 2000
SEED = 20261124  # fixed: the same export gives the same report

# The tables and columns read here, checked against the export's manifest. Exports made
# before week 5, step 5 lack the per-event tables and are refused with their names.
CONTRACT: dict[str, frozenset[str]] = {
    "fct_pump_events": frozenset(
        {"pump_event_id", "symbol", "event_hour", "event_scope", "market_breadth", "return_1h"}
    ),
    "fct_pump_event_excess": frozenset(
        {
            "pump_event_id",
            "offset_hours",
            "metric",
            "value",
            "control_hours",
            "control_median",
            "control_share_above_zero",
            "control_share_below_zero",
            "excess",
        }
    ),
    "mart_pump_signal_check": frozenset(
        {"rule", "signal_date", "signal_hours", "followed_by_pump"}
    ),
}
BASE_RULE = "every_hour"

# How a metric reads: "%" a change (fraction), "pp" a share (fraction, differences in
# percentage points), "x" a ratio, "bp" a rate in basis points.
UNITS = {"volume_ratio": "x", "taker_buy_share": "pp", "funding_premium_8h": "bp"}
LABELS = {
    "volume_ratio": "Volume vs 7-day baseline",
    "return_since_start": "Price vs the start",
    "taker_buy_share": "Taker buy share",
    "return_since_anchor_close": "Price vs the pump hour's close",
    "funding_premium_8h": "Funding premium per 8 h",
    "open_interest_change_since_start": "Open interest vs the start",
    "top_position_ratio_change_since_start": "Top traders' long/short (positions) vs the start",
    "top_account_ratio_change_since_start": "Top traders' long/short (accounts) vs the start",
    "global_account_ratio_change_since_start": "All accounts' long/short vs the start",
}


@dataclass(frozen=True)
class Inputs:
    export_id: str
    data_as_of: datetime | None  # end of the last candle, UTC
    events: pd.DataFrame  # one row per pump, with event_day (UTC date of the pump hour)
    excess: pd.DataFrame  # fct_pump_event_excess joined with event_day and event_scope
    signal: pd.DataFrame  # mart_pump_signal_check


@dataclass(frozen=True)
class Estimate:
    value: float
    low: float  # 95 % interval, bootstrap over days
    high: float


@dataclass(frozen=True)
class ExcessLine:
    metric: str
    offset: int
    events: int
    days: int
    event_median: float
    control_median: float  # median over events of their own controls' median
    excess: Estimate  # median over events of (value - own controls' median)


@dataclass(frozen=True)
class ShareLine:
    metric: str
    offset: int
    side: str  # "above" or "below" zero
    events: int
    days: int
    event_share: float  # share of events on that side of zero
    control_share: float  # mean over events of their controls' share on that side
    difference: Estimate  # mean over events of (on that side - controls' share)


@dataclass(frozen=True)
class SignalLine:
    rule: str
    hours: int
    pumps: int  # hours followed by a pump
    precision: float
    lift: Estimate  # precision / base rate, both over the same resampled days
    recall: float  # share of all pumps that the rule caught


# --- loading ------------------------------------------------------------------------------


def load(root: Path) -> Inputs:
    """The newest complete export under root, checked against CONTRACT."""
    snapshot = data.latest_snapshot(root, required=CONTRACT, optional=data.OPTIONAL_COLUMNS)
    if snapshot is None:
        raise data.SnapshotError(f"No complete export under {root}")
    with data.open_snapshot(snapshot) as con:
        events = data.frame(
            con,
            """
            SELECT pump_event_id, symbol, event_hour, event_scope, market_breadth, return_1h,
                   CAST(event_hour AS DATE) AS event_day
            FROM fct_pump_events
            """,
        )
        excess = data.frame(
            con,
            """
            SELECT x.*, e.event_scope, CAST(e.event_hour AS DATE) AS event_day
            FROM fct_pump_event_excess AS x
            JOIN fct_pump_events AS e USING (pump_event_id)
            """,
        )
        signal = data.frame(con, "SELECT * FROM mart_pump_signal_check")
        as_of = data.data_as_of(con, snapshot)
    return Inputs(snapshot.export_id, as_of, events, excess, signal)


# --- statistics ---------------------------------------------------------------------------


def day_bootstrap(
    days: Sequence[object] | pd.Series,
    statistic: Callable[[np.ndarray], float],
    n_boot: int = N_BOOT,
    seed: int = SEED,
) -> Estimate:
    """statistic(row indices) on all rows, and a 95 % percentile interval over n_boot
    resamples of whole days with replacement: a resampled day brings all its rows."""
    codes, uniques = pd.factorize(np.asarray(days, dtype=object))
    if len(uniques) == 0:
        return Estimate(np.nan, np.nan, np.nan)
    groups = [np.flatnonzero(codes == i) for i in range(len(uniques))]
    rng = np.random.default_rng(seed)
    replicates = np.empty(n_boot)
    for b in range(n_boot):
        picked = rng.integers(0, len(groups), size=len(groups))
        replicates[b] = statistic(np.concatenate([groups[i] for i in picked]))
    if np.isnan(replicates).all():
        low = high = np.nan
    else:
        low, high = np.nanpercentile(replicates, [2.5, 97.5])
    return Estimate(float(statistic(np.arange(len(codes)))), float(low), float(high))


def select(excess: pd.DataFrame, metric: str, offset: int, scope: str = "all") -> pd.DataFrame:
    mask = (excess["metric"] == metric) & (excess["offset_hours"] == offset)
    if scope != "all":
        mask &= excess["event_scope"] == scope
    return excess.loc[mask].reset_index(drop=True)


def values(rows: pd.DataFrame, column: str) -> np.ndarray:
    return rows[column].to_numpy(dtype=float)


def median(array: np.ndarray) -> float:
    return float(np.median(array)) if len(array) else np.nan


def mean(array: np.ndarray) -> float:
    return float(array.mean()) if len(array) else np.nan


def excess_line(
    excess: pd.DataFrame, metric: str, offset: int, scope: str = "all", n_boot: int = N_BOOT
) -> ExcessLine:
    """Events with a value and at least one control value at that offset."""
    rows = select(excess, metric, offset, scope)
    rows = rows.loc[rows["excess"].notna()]
    diffs = values(rows, "excess")
    days = rows["event_day"].tolist()
    return ExcessLine(
        metric,
        offset,
        len(rows),
        len(set(days)),
        median(values(rows, "value")),
        median(values(rows, "control_median")),
        day_bootstrap(days, lambda i: median(diffs[i]), n_boot),
    )


def share_line(
    excess: pd.DataFrame,
    metric: str,
    offset: int,
    side: str,
    scope: str = "all",
    n_boot: int = N_BOOT,
) -> ShareLine:
    """How often the event is above (or below) zero against how often its own controls
    are: the per-event difference (1 or 0) - control share, averaged."""
    if side not in ("above", "below"):
        raise ValueError(f"side must be 'above' or 'below', not {side!r}")
    rows = select(excess, metric, offset, scope)
    column = f"control_share_{side}_zero"
    rows = rows.loc[rows[column].notna()]
    event_values = values(rows, "value")
    on_side = ((event_values > 0) if side == "above" else (event_values < 0)).astype(float)
    control_share = values(rows, column)
    diffs = on_side - control_share
    days = rows["event_day"].tolist()
    return ShareLine(
        metric,
        offset,
        side,
        len(rows),
        len(set(days)),
        mean(on_side),
        mean(control_share),
        day_bootstrap(days, lambda i: mean(diffs[i]), n_boot),
    )


def ratio(numerator: np.ndarray, denominator: np.ndarray) -> float:
    total = denominator.sum()
    return float(numerator.sum() / total) if total else np.nan


def signal_lines(signal: pd.DataFrame, n_boot: int = N_BOOT) -> list[SignalLine]:
    """Precision, lift and recall per rule. Days are the base rule's days; a day where a
    rule never held counts as 0 hours, so every replicate compares the same days."""
    base = signal[signal["rule"] == BASE_RULE].set_index("signal_date")
    if base.empty:
        raise ValueError(f"mart_pump_signal_check has no rule {BASE_RULE}")
    all_hours = base["signal_hours"].to_numpy(dtype=float)
    all_pumps = base["followed_by_pump"].to_numpy(dtype=float)
    lines = []
    rules = [BASE_RULE] + sorted(set(signal["rule"]) - {BASE_RULE})
    for rule in rules:
        own = signal[signal["rule"] == rule].set_index("signal_date")
        own = own[["signal_hours", "followed_by_pump"]].reindex(base.index, fill_value=0)
        hours = own["signal_hours"].to_numpy(dtype=float)
        pumps = own["followed_by_pump"].to_numpy(dtype=float)

        def lift(i: np.ndarray, hours: np.ndarray = hours, pumps: np.ndarray = pumps) -> float:
            base_rate = ratio(all_pumps[i], all_hours[i])  # 0 on resampled pump-free days
            return ratio(pumps[i], hours[i]) / base_rate if base_rate else np.nan

        lines.append(
            SignalLine(
                rule,
                int(hours.sum()),
                int(pumps.sum()),
                ratio(pumps, hours),
                day_bootstrap(base.index.to_series(), lift, n_boot),
                ratio(pumps, all_pumps),
            )
        )
    return lines


# --- the report ---------------------------------------------------------------------------


def fmt(value: float, unit: str, signed: bool = False) -> str:
    if value is None or np.isnan(value):
        return "–"
    sign = "+" if signed else ""
    if unit == "x":
        return f"{value:{sign}.2f}" if signed else f"{value:.2f}×"
    if unit == "bp":
        return f"{value * 10_000:{sign}.2f} bp"
    if unit == "pp":
        return f"{value * 100:{sign}.1f} p.p." if signed else f"{value * 100:.1f} %"
    return f"{value * 100:{sign}.2f} %"


def fmt_estimate(estimate: Estimate, unit: str) -> str:
    return (
        f"{fmt(estimate.value, unit, True)} "
        f"[{fmt(estimate.low, unit, True)}; {fmt(estimate.high, unit, True)}]"
    )


def table(header: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def excess_rows(lines: Sequence[ExcessLine]) -> list[list[str]]:
    rows = []
    for line in lines:
        unit = UNITS.get(line.metric, "%")
        rows.append(
            [
                LABELS.get(line.metric, line.metric),
                f"{line.offset:+d} h",
                str(line.events),
                str(line.days),
                fmt(line.event_median, unit),
                fmt(line.control_median, unit),
                fmt_estimate(line.excess, unit),
            ]
        )
    return rows


EXCESS_HEADER = (
    "Metric",
    "Hour",
    "Events",
    "Days",
    "Event median",
    "Controls' median",
    "Median excess [95 % CI]",
)


def overview(events: pd.DataFrame) -> str:
    days = events["event_day"]
    by_scope = []
    for scope in ("all", "single_coin", "multi_coin"):
        part = events if scope == "all" else events[events["event_scope"] == scope]
        by_scope.append(
            [scope, str(len(part)), str(part["event_day"].nunique()), str(part["symbol"].nunique())]
        )
    top = events["symbol"].value_counts().head(3)
    top_share = top.sum() / len(events)
    return "\n\n".join(
        [
            f"{len(events)} pumps (an hour up ≥ the threshold, episodes 24 h apart) on "
            f"{events['symbol'].nunique()} coins over {days.nunique()} days, "
            f"{days.min():%Y-%m-%d} … {days.max():%Y-%m-%d}. Median pump hour: "
            f"{fmt(median(values(events, 'return_1h')), '%', True)}.",
            table(("Scope", "Events", "Days", "Coins"), by_scope),
            f"Top three coins ({', '.join(top.index)}): {top_share:.0%} of the events.",
        ]
    )


def render(inputs: Inputs, n_boot: int = N_BOOT) -> str:
    x = inputs.excess
    before = [excess_line(x, "volume_ratio", k, n_boot=n_boot) for k in (-6, -3, -2, -1, 0)] + [
        excess_line(x, "return_since_start", -6, n_boot=n_boot),
        excess_line(x, "return_since_start", 0, n_boot=n_boot),
        excess_line(x, "taker_buy_share", -3, n_boot=n_boot),
        excess_line(x, "taker_buy_share", -1, n_boot=n_boot),
        excess_line(x, "taker_buy_share", 0, n_boot=n_boot),
    ]

    after_rows = []
    for scope in ("all", "single_coin", "multi_coin"):
        for k in (1, 4, 24):
            ex = excess_line(x, "return_since_anchor_close", k, scope, n_boot)
            up = share_line(x, "return_since_anchor_close", k, "above", scope, n_boot)
            after_rows.append(
                [
                    scope,
                    f"+{k} h",
                    str(ex.events),
                    str(ex.days),
                    fmt(ex.event_median, "%", True),
                    fmt_estimate(ex.excess, "%"),
                    f"{up.event_share:.0%} / {up.control_share:.0%}",
                    fmt_estimate(up.difference, "pp"),
                ]
            )

    signal_rows = [
        [
            line.rule,
            f"{line.hours:,}",
            str(line.pumps),
            f"{line.precision:.2%}",
            "1 (base rate)"
            if line.rule == BASE_RULE
            else f"{line.lift.value:.1f}× [{line.lift.low:.1f}; {line.lift.high:.1f}]",
            f"{line.recall:.0%}",
        ]
        for line in signal_lines(inputs.signal, n_boot)
    ]

    funding = excess_line(x, "funding_premium_8h", -1, n_boot=n_boot)
    funding_share = share_line(x, "funding_premium_8h", -1, "below", n_boot=n_boot)
    futures = [
        excess_line(x, metric, k, n_boot=n_boot)
        for metric in (
            "open_interest_change_since_start",
            "top_position_ratio_change_since_start",
            "top_account_ratio_change_since_start",
            "global_account_ratio_change_since_start",
        )
        for k in (-24, -6, 0, 24)
    ]

    as_of = "unknown" if inputs.data_as_of is None else f"{inputs.data_as_of:%Y-%m-%d %H:%M} UTC"
    return "\n\n".join(
        [
            f"# Pump findings: export {inputs.export_id}",
            f"Data to {as_of}. Bootstrap: {n_boot} resamples of event days, seed {SEED}. "
            "Excess = event value - median of the event's own control hours (ordinary hours "
            "of the same coin). Offsets: hour 0 is the pump hour; values marked "
            '"vs the start" are against the end of hour -1.',
            "## 1. The events",
            overview(inputs.events),
            "## 2. Before the pump",
            table(EXCESS_HEADER, excess_rows(before)),
            "## 3. After the pump hour closes",
            "Price against the close of the pump hour: what buying at its end would have "
            "seen. Share up: events / their own controls.",
            table(
                (
                    "Scope",
                    "After",
                    "Events",
                    "Days",
                    "Event median",
                    "Median excess [95 % CI]",
                    "Share up",
                    "Difference [95 % CI]",
                ),
                after_rows,
            ),
            "## 4. As a signal: is the next hour a pump?",
            "Hours t where the rule held, and how many were followed by an hour up ≥ the "
            "threshold. Lift = precision / base rate; recall = share of all such hours caught.",
            table(
                ("Rule", "Hours", "Followed by a pump", "Precision", "Lift [95 % CI]", "Recall"),
                signal_rows,
            ),
            "## 5. Funding and futures positioning",
            table(EXCESS_HEADER, excess_rows([funding])),
            f"Funding premium below zero at the start: {funding_share.event_share:.0%} of "
            f"{funding_share.events} events against {funding_share.control_share:.0%} of their "
            f"controls; difference {fmt_estimate(funding_share.difference, 'pp')}.",
            "Futures metrics exist for about 30 days (recent control pool):",
            table(EXCESS_HEADER, excess_rows(futures)),
        ]
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pump findings from the newest mart export.")
    parser.add_argument("--marts", type=Path, default=MARTS_DIR, help="export root")
    parser.add_argument("--boot", type=int, default=N_BOOT, help="bootstrap resamples")
    args = parser.parse_args(argv)
    if args.boot < 100:
        parser.error("--boot must be at least 100")
    try:
        inputs = load(args.marts)
    except data.SnapshotError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]  # ≥, …, × on Windows
    print(render(inputs, args.boot))
    return 0


if __name__ == "__main__":
    sys.exit(main())
