"""Streamlit dashboard over the newest mart export: pumps, the +-24 h profile, data quality.

Run from the repo root: python -m streamlit run dashboard/app.py
(MARTS_DIR overrides data/marts; in compose the service mounts it read-only.)
The page only lays out what dashboard.data returns; the logic and its tests live there.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from dashboard import data
from dashboard.data import PumpFilter, Snapshot

MARTS_DIR = Path(
    os.environ.get("MARTS_DIR", Path(__file__).resolve().parents[1] / "data" / "marts")
)

# Chart colors (reference palette, dataviz skill): the event is the series, the control
# a neutral reference; scopes take the first two categorical slots.
EVENT_COLOR = "#2a78d6"
CONTROL_COLOR = "#898781"
SCOPE_COLORS = {"single_coin": "#2a78d6", "multi_coin": "#eb6834"}
SERIES_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6"]
# Vega-Lite draws times in the viewer's zone unless told otherwise; the pipeline, the
# schedule and the data are UTC, so the charts are too.
UTC_SCALE = alt.Scale(type="utc")
STATUS_LABELS = {
    "pass": "✅ pass",
    "warn": "⚠️ warn",
    "fail": "❌ fail",
    "error": "❌ error",
    "skipped": "⏭️ skipped",
}

# What the profile says per metric: the findings (docs/findings.md, analysis/).
NOTES = {
    "return_since_start": (
        "**The move is the pump hour itself**, after a quiet run-up: the price is about "
        "1.6 % higher over the 6 hours before the start than ordinary hours would give. "
        "See *Price after the pump hour* for what follows."
    ),
    "return_since_anchor_close": (
        "**No follow-through.** Measured from the close of the pump hour, the median "
        "barely moves over 24 hours and is up about as often as ordinary hours are. "
        "Buying at the end of the pump hour has no edge even before costs."
    ),
    "volume_ratio": (
        "**The footprint before the pump.** Volume runs above its 7-day baseline for the "
        "three hours before the pump, while ordinary hours sit below it. As a predictor it "
        "is weak: a pump follows 0.13 % of hours, so almost every volume spike is a false "
        "alarm."
    ),
    "taker_buy_share": (
        "Aggressive buying shows in the pump hour only; before it the taker share stays "
        "within the range of ordinary hours."
    ),
    "funding_premium_8h": (
        "**No short squeeze in funding.** The premium is below zero before most pumps, but "
        "just as often in ordinary hours: shorts paying longs is the norm, not a setup."
    ),
    "open_interest_change_since_start": (
        "About 30 days of open-interest history: too few events for a conclusion."
    ),
}
RATIO_NOTE = (
    "**A hypothesis, not a finding.** Before the pumps of November 2026 the top traders' "
    "long/short position ratio was about 8 % higher 24 hours earlier than at the start, "
    "while ordinary hours stayed flat: they leaned short into the rise. The events with "
    "long/short history (since 2026-10-23) are 28 on 6 days, mostly three market "
    "rallies: a few observations, not dozens."
)


def number(label: str, fmt: str | None = None) -> Any:
    return st.column_config.NumberColumn(label, format=fmt)


def moment(label: str) -> Any:
    return st.column_config.DatetimeColumn(label, format="YYYY-MM-DD HH:mm")


def utc_text(moment: datetime | pd.Timestamp | None) -> str:
    return "unknown" if moment is None else moment.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


@st.cache_resource(max_entries=2, show_spinner=False)
def connection(directory: str) -> duckdb.DuckDBPyConnection:
    """One in-memory copy per export, shared by the sessions; each run takes a cursor."""
    return data.open_snapshot(data.read_snapshot(Path(directory)))


def header(con: duckdb.DuckDBPyConnection, snapshot: Snapshot) -> None:
    st.title("Crypto market pumps")
    st.caption(
        "What happens to volume, funding and positioning in the 24 hours around a rise of "
        "5 % or more in one hour, on 23 liquid coins of Binance (spot and USD-M futures)."
    )
    as_of = data.data_as_of(con, snapshot)
    st.markdown(
        f"Data as of **{utc_text(as_of)}** (end of the last hourly candle) · "
        f"exported {utc_text(snapshot.exported_at)} · export `{snapshot.export_id}`"
    )
    now = datetime.now(UTC)
    if data.is_stale(snapshot, now):
        hours = data.snapshot_age(snapshot, now).total_seconds() / 3600
        st.warning(
            f"The export is {hours:.0f} hours old; the pipeline exports every 3 hours. "
            "Check the crypto_pipeline DAG."
        )


# --- pumps -------------------------------------------------------------------------------


def pumps_tab(con: duckdb.DuckDBPyConnection) -> None:
    dates = data.event_date_range(con)
    if dates is None:
        st.info("No pump events in this export.")
        return
    symbols = data.symbol_options(con)
    counts = dict(zip(symbols["symbol"], symbols["events"], strict=True))

    left, middle, right = st.columns([2, 3, 2])
    scope = left.radio(
        "Scope", list(data.SCOPES), format_func=data.SCOPES.__getitem__, key="pumps_scope"
    )
    chosen = middle.multiselect(
        "Symbols (all when empty)",
        symbols["symbol"].tolist(),
        format_func=lambda s: f"{s} ({counts[s]})",
    )
    picked = right.date_input(
        "Event dates (UTC)", value=dates, min_value=dates[0], max_value=dates[1]
    )
    left2, right2 = st.columns([3, 2])
    min_rise = left2.slider("Minimum rise in the hour, %", 5.0, 25.0, 5.0, 0.5)
    exclude = right2.checkbox(
        "Leave out events whose window overlaps the previous one of the coin",
        help="Another event of the same coin less than 48 hours earlier.",
    )
    date_from, date_to = (picked[0], picked[-1]) if isinstance(picked, tuple) and picked else dates

    events = data.pump_events(
        con,
        PumpFilter(
            scope=scope,
            symbols=tuple(chosen),
            date_from=date_from,
            date_to=date_to,
            min_return=min_rise / 100,
            exclude_overlaps=exclude,
        ),
    )
    summary = data.pump_summary(events)
    cells = st.columns(5)
    cells[0].metric("Events", summary["events"])
    cells[1].metric("Days with events", summary["days"])
    cells[2].metric("Coins", summary["symbols"])
    cells[3].metric("Single coin / market", f"{summary['single_coin']} / {summary['multi_coin']}")
    median = summary["median_return_1h"]
    cells[4].metric("Median rise", "–" if median is None else f"{median:.1%}")
    st.caption(
        "Coins that rise in the same hour are one market move: days, not events, are the "
        "independent observations."
    )
    if events.empty:
        st.info("No events match the filters.")
        return

    table = data.events_table(events)
    timeline = (
        alt.Chart(table.assign(hour_utc=table["event_hour"].map(utc_text)))
        .mark_circle(size=80, opacity=0.85, stroke="white", strokeWidth=1)
        .encode(
            x=alt.X(
                "event_hour:T",
                title="UTC",
                scale=UTC_SCALE,
                # One tick a month: the default weekly ticks repeat "Dec 2025" four times.
                axis=alt.Axis(format="%b %Y", tickCount="month", labelOverlap=True),
            ),
            y=alt.Y("return_1h:Q", title="Rise in the hour, %"),
            color=alt.Color(
                "scope_label:N",
                title="Scope",
                scale=alt.Scale(
                    domain=[data.SCOPES[s] for s in SCOPE_COLORS],
                    range=list(SCOPE_COLORS.values()),
                ),
                legend=alt.Legend(orient="top"),
            ),
            tooltip=[
                alt.Tooltip("hour_utc:N", title="Hour"),
                alt.Tooltip("symbol:N", title="Coin"),
                alt.Tooltip("return_1h:Q", title="Rise, %", format=".1f"),
                alt.Tooltip("market_breadth:Q", title="Coins rising that hour"),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(timeline, width="stretch")

    st.dataframe(
        table,
        hide_index=True,
        width="stretch",
        column_order=[
            "event_hour",
            "symbol",
            "scope_label",
            "market_breadth",
            "return_1h",
            "return_4h",
            "return_before",
            "return_after",
            "volume_ratio_before",
            "volume_ratio_event",
            "volume_ratio_after",
            "open_interest_change_before",
            "open_interest_change_after",
            "funding_rate_8h_before",
            "episode_hours",
            "overlaps_previous_event",
        ],
        column_config={
            "event_hour": moment("Hour (UTC)"),
            "symbol": "Coin",
            "scope_label": "Scope",
            "market_breadth": number("Coins that hour"),
            "return_1h": number("Rise 1 h, %", "%.1f"),
            "return_4h": number("Rise 4 h, %", "%.1f"),
            "return_before": number("Price 24 h before, %", "%.1f"),
            "return_after": number("Price 24 h after, %", "%.1f"),
            "volume_ratio_before": number("Volume before, ×", "%.2f"),
            "volume_ratio_event": number("Volume in the hour, ×", "%.2f"),
            "volume_ratio_after": number("Volume after, ×", "%.2f"),
            "open_interest_change_before": number("OI before, %", "%.1f"),
            "open_interest_change_after": number("OI after, %", "%.1f"),
            "funding_rate_8h_before": number("Funding before, bp/8 h", "%.2f"),
            "episode_hours": number("Hours ≥ 5 %"),
            "overlaps_previous_event": st.column_config.CheckboxColumn("Overlaps"),
        },
    )

    st.caption(
        "None: the window has no data there. Open interest starts on 2026-10-22, and events "
        "at the edges of the history lack a full 24 hours before or after."
    )

    st.subheader("One event, hour by hour")
    labels = {
        event_id: f"{utc_text(hour)} · {symbol} · +{rise:.1f} %"
        for event_id, hour, symbol, rise in zip(
            table["pump_event_id"],
            table["event_hour"],
            table["symbol"],
            table["return_1h"],
            strict=True,
        )
    }
    event_id = st.selectbox("Event", list(labels), format_func=labels.__getitem__)
    if event_id is not None:
        event_chart(data.event_window(con, event_id))


def event_chart(window: pd.DataFrame) -> None:
    """Price and volume of one event: two charts, one axis each."""
    window = window.assign(price=window["return_since_start"] * 100, volume=window["volume_ratio"])
    x = alt.X("offset_hours:Q", title="Hours from the pump hour", scale=alt.Scale(domain=[-24, 24]))
    zero = (
        alt.Chart(pd.DataFrame({"offset_hours": [0]}))
        .mark_rule(color=CONTROL_COLOR, strokeDash=[2, 2])
        .encode(x=x)
    )
    price = (
        alt.Chart(window)
        .mark_line(color=EVENT_COLOR, strokeWidth=2, point=alt.OverlayMarkDef(size=30))
        .encode(
            x=x,
            y=alt.Y("price:Q", title="Price vs start, %"),
            tooltip=[
                alt.Tooltip("offset_hours:Q", title="Hour"),
                alt.Tooltip("price:Q", title="%", format=".2f"),
            ],
        )
    )
    volume = (
        alt.Chart(window)
        .mark_bar(color=EVENT_COLOR, opacity=0.8, cornerRadiusTopLeft=2, cornerRadiusTopRight=2)
        .encode(
            x=x,
            y=alt.Y("volume:Q", title="Volume vs 7-day baseline, ×"),
            tooltip=[
                alt.Tooltip("offset_hours:Q", title="Hour"),
                alt.Tooltip("volume:Q", title="×", format=".2f"),
            ],
        )
    )
    left, right = st.columns(2)
    left.altair_chart((zero + price).properties(height=240), width="stretch")
    right.altair_chart((zero + volume).properties(height=240), width="stretch")


# --- profile -----------------------------------------------------------------------------


def profile_tab(con: duckdb.DuckDBPyConnection) -> None:
    scopes, metrics = data.profile_options(con)
    if not metrics:
        st.info("No profile in this export.")
        return
    left, right = st.columns([2, 3])
    scope = left.radio(
        "Events", scopes, format_func=lambda s: data.SCOPES.get(s, s), key="profile_scope"
    )
    metric = right.selectbox("Metric", metrics, format_func=lambda m: data.metric_info(m).label)
    info = data.metric_info(metric)
    st.caption(info.description)

    profile = data.scaled_profile(data.pump_profile(con, scope, metric), metric)
    if profile.empty:
        st.info("No values for this metric in this scope.")
        return
    at_start = profile.loc[profile["offset_hours"] == 0]
    if not at_start.empty:
        st.markdown(
            f"**{at_start['events'].iloc[0]} pump events** against "
            f"{at_start['control_hours'].iloc[0]} ordinary hours of the same coins at "
            "the pump hour; fewer at the edges of the history."
        )
    profile_chart(profile, info)

    note = RATIO_NOTE if "_ratio_" in metric else NOTES.get(metric)
    if note:
        st.info(note)

    st.dataframe(
        data.profile_highlights(profile),
        hide_index=True,
        width="stretch",
        column_order=[
            "offset_hours",
            "events",
            "event_median",
            "event_p25",
            "event_p75",
            "control_median",
            "median_excess",
        ],
        column_config={
            "offset_hours": number("Hour"),
            "events": number("Events"),
            "event_median": number(f"Event median, {info.unit}", "%.2f"),
            "event_p25": number("25th pct", "%.2f"),
            "event_p75": number("75th pct", "%.2f"),
            "control_median": number(f"Ordinary hours, {info.unit}", "%.2f"),
            "median_excess": number("Median excess", "%.2f"),
        },
    )
    st.caption(
        "Median excess: the median over events of (event value − the median of its own "
        "ordinary hours). Hour 0 is the pump hour; values are at the end of each hour."
    )


def profile_chart(profile: pd.DataFrame, info: data.Metric) -> None:
    series = pd.concat(
        [
            profile.assign(series="Pump events (median)", value=profile["event_median"]),
            profile.assign(series="Ordinary hours (median)", value=profile["control_median"]),
        ]
    )
    x = alt.X("offset_hours:Q", title="Hours from the pump hour", scale=alt.Scale(domain=[-24, 24]))
    band = (
        alt.Chart(profile)
        .mark_area(color=EVENT_COLOR, opacity=0.15)
        .encode(x=x, y=alt.Y("event_p25:Q", title=f"{info.label}, {info.unit}"), y2="event_p75:Q")
    )
    lines = (
        alt.Chart(series)
        .mark_line(strokeWidth=2)
        .encode(
            x=x,
            y="value:Q",
            color=alt.Color(
                "series:N",
                title=None,
                scale=alt.Scale(
                    domain=["Pump events (median)", "Ordinary hours (median)"],
                    range=[EVENT_COLOR, CONTROL_COLOR],
                ),
                legend=alt.Legend(orient="top"),
            ),
            strokeDash=alt.StrokeDash(
                "series:N",
                scale=alt.Scale(
                    domain=["Pump events (median)", "Ordinary hours (median)"],
                    range=[[1, 0], [5, 3]],
                ),
                legend=None,
            ),
        )
    )
    hover = alt.selection_point(
        fields=["offset_hours"], nearest=True, on="pointerover", empty=False
    )
    points = (
        alt.Chart(profile)
        .mark_rule(color=CONTROL_COLOR, opacity=0)
        .encode(
            x=x,
            opacity=alt.condition(hover, alt.value(0.6), alt.value(0)),
            tooltip=[
                alt.Tooltip("offset_hours:Q", title="Hour"),
                alt.Tooltip("events:Q", title="Events"),
                alt.Tooltip("event_median:Q", title="Event median", format=".2f"),
                alt.Tooltip("event_p25:Q", title="25th pct", format=".2f"),
                alt.Tooltip("event_p75:Q", title="75th pct", format=".2f"),
                alt.Tooltip("control_median:Q", title="Ordinary hours", format=".2f"),
            ],
        )
        .add_params(hover)
    )
    zero = (
        alt.Chart(pd.DataFrame({"offset_hours": [0]}))
        .mark_rule(color=CONTROL_COLOR, strokeDash=[2, 2])
        .encode(x=x)
    )
    st.altair_chart((band + lines + zero + points).properties(height=340), width="stretch")
    st.caption("Band: the middle half of the events (25th to 75th percentile).")


# --- data quality ------------------------------------------------------------------------


def dq_tab(con: duckdb.DuckDBPyConnection, snapshot: Snapshot) -> None:
    build = data.latest_build(con)
    if build is None:
        st.info("No dbt build in the DQ log of this export.")
    else:
        st.markdown(f"Newest dbt build: **{utc_text(build['run_started_at'])}**")
        cells = st.columns(4)
        cells[0].metric("Checks", build["checks"])
        cells[1].metric("✅ Passed", build["passed"])
        cells[2].metric("⚠️ Warnings", build["warned"])
        cells[3].metric("❌ Failed", build["failed"])
    st.caption(
        "Stop checks protect the marts: one failing stops the pipeline before the export. "
        "Warn checks describe the source (gaps Binance never filled, outliers) and are kept "
        "as findings in the warehouse's dq schema."
    )

    coverage = data.coverage(con, snapshot)
    st.subheader("Coverage")
    if coverage is None:
        st.info("This export predates the coverage table (dq_data_coverage).")
    else:
        st.dataframe(
            coverage,
            hide_index=True,
            width="stretch",
            column_config={
                "dataset": "Dataset",
                "row_count": number("Rows"),
                "symbol_count": number("Symbols"),
                "data_from": moment("From (UTC)"),
                "data_to": moment("To (UTC)"),
                "latest_batch_id": "Newest batch",
                "hours_behind_export": number("Hours before the export", "%.1f"),
            },
        )

    checks = data.check_status(con)
    checks = checks.assign(status=checks["status"].map(lambda s: STATUS_LABELS.get(s, s)))
    problems = checks[checks["status"] != STATUS_LABELS["pass"]]
    st.subheader("Checks that need a look" if not problems.empty else "All checks pass")
    check_columns = {
        "check_label": "Check",
        "status": "Status",
        "failures": number("Rows found"),
        "severity": "Severity",
        "model_name": "Model",
        "failures_relation": "Findings table",
        "check_type": "Type",
        "logged_at": moment("Logged (UTC)"),
        "in_latest_build": st.column_config.CheckboxColumn("In newest build"),
    }
    if not problems.empty:
        st.dataframe(problems, hide_index=True, width="stretch", column_config=check_columns)
    with st.expander(f"All {len(checks)} checks, latest result of each"):
        st.dataframe(checks, hide_index=True, width="stretch", column_config=check_columns)

    st.subheader("Warn findings over time")
    history = data.warn_history(con)
    if history.empty:
        st.info("No warn check has found anything so far.")
        return
    labels = sorted(history["check_label"].unique())
    chart = (
        alt.Chart(history.assign(build_utc=history["run_started_at"].map(utc_text)))
        .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=40))
        .encode(
            x=alt.X(
                "run_started_at:T",
                title="dbt build, UTC",
                scale=UTC_SCALE,
                axis=alt.Axis(format="%d %b %H:%M"),
            ),
            y=alt.Y("failures:Q", title="Rows found"),
            color=alt.Color(
                "check_label:N",
                title=None,
                scale=alt.Scale(domain=labels, range=SERIES_COLORS[: len(labels)]),
                legend=alt.Legend(orient="top"),
            ),
            tooltip=[
                alt.Tooltip("build_utc:N", title="Build"),
                alt.Tooltip("check_label:N", title="Check"),
                alt.Tooltip("failures:Q", title="Rows found"),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, width="stretch")


# --- forward test ------------------------------------------------------------------------


def signals_tab(con: duckdb.DuckDBPyConnection, snapshot: Snapshot) -> None:
    st.caption(
        "Forward test of the signal versions registered on 2026-11-28 (docs/forward_test.md): "
        "decided at 00:07 UTC from the closed day, entered at 01:00 UTC, logged before the "
        "entry in a chained, append-only log. A paper test until a version runs 183 days with "
        "100 closed trades and a 95 % interval above 0: not a confirmed signal."
    )
    performance = data.signal_performance(con, snapshot)
    log = data.signal_log(con, snapshot)
    if performance is None or log is None:
        st.info("This export has no forward-test tables yet.")
        return
    st.subheader("Results per version")
    st.dataframe(
        performance,
        hide_index=True,
        column_config={
            c: st.column_config.NumberColumn(format="percent")
            for c in (
                "hit_rate",
                "mean_net",
                "ci_low",
                "ci_high",
                "mean_excess",
                "survive_3x_share",
                "worst_net",
            )
        },  # fmt: skip
    )
    st.subheader("Decisions")
    if log.empty:
        st.info("Nothing logged yet: the first decision day is 2026-11-29.")
        return
    st.dataframe(
        log,
        hide_index=True,
        column_config={
            c: st.column_config.NumberColumn(format="percent")
            for c in ("stop_loss", "net_return", "mae", "excess_return")
        },
    )


def main() -> None:
    st.set_page_config(page_title="Crypto market pumps", page_icon="📈", layout="wide")
    try:
        snapshot = data.latest_snapshot(MARTS_DIR)
    except data.SnapshotError as exc:
        st.error(f"Cannot read the newest export: {exc}")
        st.stop()
    if snapshot is None:
        st.info(
            f"No complete export in {MARTS_DIR} yet: the pipeline writes one after each "
            "successful run (task export_marts)."
        )
        st.stop()
    try:
        con = connection(str(snapshot.directory)).cursor()
    except data.SnapshotError as exc:
        st.error(f"Cannot load the newest export: {exc}")
        st.stop()
    try:
        header(con, snapshot)
        pumps, profile, quality, signals = st.tabs(
            ["Pumps", "Profile ±24 h", "Data quality", "Signals (paper test)"]
        )
        with pumps:
            pumps_tab(con)
        with profile:
            profile_tab(con)
        with quality:
            dq_tab(con, snapshot)
        with signals:
            signals_tab(con, snapshot)
    finally:
        con.close()


main()
