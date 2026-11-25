"""Read the newest complete mart export and shape it for the dashboard.

The export contract (ingestion/export_marts.py): data/marts/<export_id>/<table>.parquet,
the manifest _manifest.json written last. A directory without the manifest is an export
in progress and is never read. Everything here is a plain function over a Snapshot, a
DuckDB connection or a DataFrame; the Streamlit page (app.py) only lays out the results.

The dashboard reads files, never the warehouse: DuckDB lets one process hold its file
while the pipeline writes it, and the files are the same whatever engine builds the
marts. Numbers arrive as DOUBLE: fine for display and charts, the stored facts stay
decimal in the files where the warehouse keeps them decimal.
"""

from __future__ import annotations

import json
import re
import statistics
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

MANIFEST_NAME = "_manifest.json"
EXPORT_ID = re.compile(r"\d{8}T\d{6}Z")  # the UTC as-of moment, like a landing-zone batch

# Columns the queries below read, per table: checked against the manifest before loading,
# so an export with a renamed or dropped column fails with its name, not with a SQL error.
REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "dim_symbol": frozenset({"symbol", "has_perpetual"}),
    "fct_pump_events": frozenset(
        {
            "pump_event_id",
            "symbol",
            "event_hour",
            "event_scope",
            "market_breadth",
            "episode_hours",
            "overlaps_previous_event",
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
        }
    ),
    "fct_pump_event_window": frozenset(
        {
            "pump_event_id",
            "offset_hours",
            "open_time",
            "return_since_start",
            "volume_ratio",
            "taker_buy_share",
            "funding_premium_8h",
            "open_interest_change_since_start",
        }
    ),
    "mart_pump_profile": frozenset(
        {
            "event_scope",
            "offset_hours",
            "metric",
            "events",
            "event_median",
            "event_p25",
            "event_p75",
            "control_hours",
            "control_median",
            "median_excess",
        }
    ),
    "dq_check_results": frozenset(
        {
            "invocation_id",
            "run_started_at",
            "dbt_command",
            "check_unique_id",
            "check_name",
            "check_type",
            "model_name",
            "severity",
            "status",
            "failures",
            "failures_relation",
            "logged_at",
            "is_latest",
        }
    ),
}
# Exports made before a table existed lack it; the page shows what it can without it.
OPTIONAL_COLUMNS: dict[str, frozenset[str]] = {
    "dq_data_coverage": frozenset(
        {"dataset", "row_count", "symbol_count", "data_from", "data_to", "latest_batch_id"}
    ),
}
COVERAGE = "dq_data_coverage"
DATA_AS_OF_DATASET = "klines_1h"  # prices: the dataset every mart is built on

# The pipeline exports every 3 hours; one missed run plus the watchdog's hour of grace.
STALE_AFTER = timedelta(hours=4)


class SnapshotError(Exception):
    """The newest complete export cannot be read as the dashboard expects."""


@dataclass(frozen=True)
class Snapshot:
    export_id: str
    directory: Path
    exported_at: datetime  # aware, UTC
    rows: dict[str, int]  # per table present, from the manifest

    @property
    def tables(self) -> tuple[str, ...]:
        return tuple(self.rows)


def is_export_id(name: str) -> bool:
    return EXPORT_ID.fullmatch(name) is not None


Contract = Mapping[str, frozenset[str]]  # table -> the columns a reader needs


def latest_snapshot(
    root: Path, required: Contract = REQUIRED_COLUMNS, optional: Contract = OPTIONAL_COLUMNS
) -> Snapshot | None:
    """The newest complete export under root, or None if there is none yet.

    Complete: named like an export and holding the manifest. The newest complete one is
    read even when it is broken, and a newer export this process may not read is an error
    too: SnapshotError, rather than older data that would look current. The contract is
    the dashboard's unless a reader passes its own (analysis/pump_findings.py).
    """
    if not root.is_dir():
        return None
    exports = sorted(
        (d for d in root.iterdir() if d.is_dir() and is_export_id(d.name)), reverse=True
    )
    for directory in exports:
        if has_manifest(directory):
            return read_snapshot(directory, required, optional)
    return None


def has_manifest(directory: Path) -> bool:
    """False for an export still being written. Not Path.is_file(): it would also say
    False for a directory this process may not read, and the page would quietly fall
    back to an older export."""
    try:
        (directory / MANIFEST_NAME).stat()
    except FileNotFoundError:
        return False
    except PermissionError as exc:
        raise SnapshotError(f"Export {directory.name}: no permission to read it") from exc
    return True


def read_snapshot(
    directory: Path, required: Contract = REQUIRED_COLUMNS, optional: Contract = OPTIONAL_COLUMNS
) -> Snapshot:
    """Check an export's manifest against what a reader needs: the required tables must
    be there, an optional table only when listed; each with its columns and its file."""
    name = directory.name
    try:
        manifest = json.loads((directory / MANIFEST_NAME).read_text(encoding="utf-8"))
        export_id = manifest["export_id"]
        exported_at = datetime.fromisoformat(manifest["exported_at_utc"])
        listed = manifest["tables"]
        if not isinstance(listed, dict):
            raise TypeError("tables is not a mapping")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise SnapshotError(f"Export {name}: unreadable manifest ({exc})") from exc
    if export_id != name:
        raise SnapshotError(f"Export {name}: the manifest is of export {export_id}")
    if exported_at.utcoffset() is None:
        raise SnapshotError(f"Export {name}: exported_at_utc has no time zone")

    missing = [table for table in required if table not in listed]
    if missing:
        raise SnapshotError(f"Export {name}: no table {', '.join(missing)}")
    wanted = {**required, **{t: c for t, c in optional.items() if t in listed}}
    rows: dict[str, int] = {}
    for table, columns in wanted.items():
        entry = listed[table]
        absent = sorted(columns - set(entry.get("columns", {})))
        if absent:
            raise SnapshotError(f"Export {name}: {table} has no column {', '.join(absent)}")
        if not (directory / f"{table}.parquet").is_file():
            raise SnapshotError(f"Export {name}: file {table}.parquet is missing")
        rows[table] = int(entry["rows"])
    return Snapshot(export_id, directory, exported_at.astimezone(UTC), rows)


def open_snapshot(snapshot: Snapshot) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB holding the snapshot's tables, row counts checked.

    The files are read once, here; afterwards the connection cannot touch files at all
    (external access off, configuration locked), so every query sees this snapshot even
    if the export is pruned meanwhile. Use .cursor() per thread.
    """
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET TimeZone = 'UTC'")
        for table in snapshot.tables:  # names from a reader's contract, checked by the manifest
            path = snapshot.directory / f"{table}.parquet"
            try:
                con.read_parquet(str(path)).create(table)
            except (duckdb.IOException, duckdb.InvalidInputException) as exc:  # unreadable, damaged
                raise SnapshotError(
                    f"Export {snapshot.export_id}: cannot read {path.name} ({exc})"
                ) from exc
            loaded = scalar(con, f"SELECT count(*) FROM {table}")
            if loaded != snapshot.rows[table]:
                raise SnapshotError(
                    f"Export {snapshot.export_id}: {table} has {loaded} rows, "
                    f"the manifest {snapshot.rows[table]}"
                )
        con.execute("SET enable_external_access = false")
        con.execute("SET lock_configuration = true")
    except BaseException:
        con.close()
        raise
    return con


def first_row(con: duckdb.DuckDBPyConnection, sql: str, params: Any = None) -> tuple | None:
    """The first row of a result, or None. fetchall, not fetchone: DuckDB 2 keeps the
    connection busy until a result is read to the end."""
    rows = con.execute(sql, params).fetchall()
    return rows[0] if rows else None


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: Any = None) -> Any:
    row = first_row(con, sql, params)
    return None if row is None else row[0]


def frame(con: duckdb.DuckDBPyConnection, sql: str, params: Any = None) -> pd.DataFrame:
    return con.execute(sql, params).df()


# --- freshness ---------------------------------------------------------------------------


def snapshot_age(snapshot: Snapshot, now: datetime) -> timedelta:
    if now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return now - snapshot.exported_at


def is_stale(snapshot: Snapshot, now: datetime) -> bool:
    return snapshot_age(snapshot, now) > STALE_AFTER


def coverage(con: duckdb.DuckDBPyConnection, snapshot: Snapshot) -> pd.DataFrame | None:
    """How far each dataset reaches, and how old its last point was at the export."""
    if COVERAGE not in snapshot.tables:
        return None
    return frame(
        con,
        """
        SELECT
            dataset, row_count, symbol_count, data_from, data_to, latest_batch_id,
            date_diff('minute', data_to, $exported_at) / 60.0 AS hours_behind_export
        FROM dq_data_coverage
        ORDER BY dataset = $prices DESC, dataset
        """,
        {"exported_at": snapshot.exported_at, "prices": DATA_AS_OF_DATASET},
    )


def data_as_of(con: duckdb.DuckDBPyConnection, snapshot: Snapshot) -> datetime | None:
    """End of the last closed hourly candle in the marts, or None if the export lacks it."""
    if COVERAGE not in snapshot.tables:
        return None
    value = scalar(
        con,
        "SELECT data_to FROM dq_data_coverage WHERE dataset = $dataset",
        {"dataset": DATA_AS_OF_DATASET},
    )
    return None if value is None else value.astimezone(UTC)


# --- pumps -------------------------------------------------------------------------------

SCOPES = {
    "all": "All events",
    "single_coin": "Single coin",
    "multi_coin": "Market move (2+ coins)",
}


@dataclass(frozen=True)
class PumpFilter:
    scope: str = "all"
    symbols: tuple[str, ...] = ()  # empty: every symbol
    date_from: date | None = None  # UTC dates, both ends included
    date_to: date | None = None
    min_return: float = 0.0  # fraction, on top of the marts' 5 % rule
    exclude_overlaps: bool = False

    def __post_init__(self) -> None:
        if self.scope not in SCOPES:
            raise ValueError(f"Unknown scope {self.scope!r}")


def symbol_options(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Every whitelisted symbol with its number of events (0 for a symbol without one)."""
    return frame(
        con,
        """
        SELECT s.symbol, s.has_perpetual, count(e.pump_event_id) AS events
        FROM dim_symbol AS s
        LEFT JOIN fct_pump_events AS e ON e.symbol = s.symbol
        GROUP BY s.symbol, s.has_perpetual
        ORDER BY events DESC, s.symbol
        """,
    )


def event_date_range(con: duckdb.DuckDBPyConnection) -> tuple[date, date] | None:
    row = first_row(con, "SELECT min(event_hour)::DATE, max(event_hour)::DATE FROM fct_pump_events")
    if row is None or row[0] is None:
        return None
    return row[0], row[1]


def pump_events(con: duckdb.DuckDBPyConnection, flt: PumpFilter) -> pd.DataFrame:
    """Events matching the filter, newest first."""
    return frame(
        con,
        """
        SELECT
            pump_event_id, event_hour, symbol, event_scope, market_breadth, episode_hours,
            return_1h, return_4h, return_before, return_after,
            volume_ratio_before, volume_ratio_event, volume_ratio_after,
            open_interest_change_before, open_interest_change_after,
            funding_rate_8h_before, overlaps_previous_event
        FROM fct_pump_events
        WHERE ($scope = 'all' OR event_scope = $scope)
            AND (len($symbols::VARCHAR[]) = 0 OR list_contains($symbols::VARCHAR[], symbol))
            AND ($date_from::DATE IS NULL OR event_hour::DATE >= $date_from::DATE)
            AND ($date_to::DATE IS NULL OR event_hour::DATE <= $date_to::DATE)
            AND return_1h >= $min_return
            AND NOT ($exclude_overlaps AND overlaps_previous_event)
        ORDER BY event_hour DESC, symbol
        """,
        {
            "scope": flt.scope,
            "symbols": list(flt.symbols),
            "date_from": flt.date_from,
            "date_to": flt.date_to,
            "min_return": flt.min_return,
            "exclude_overlaps": flt.exclude_overlaps,
        },
    )


def pump_summary(events: pd.DataFrame) -> dict[str, Any]:
    """Headline counts of a pump_events result. Days count separately: events of one
    hour (a market move) are not independent observations."""
    if events.empty:
        return {
            "events": 0,
            "symbols": 0,
            "days": 0,
            "single_coin": 0,
            "multi_coin": 0,
            "median_return_1h": None,
        }
    scopes: dict[str, int] = events["event_scope"].value_counts().to_dict()
    return {
        "events": len(events),
        "symbols": events["symbol"].nunique(),
        "days": events["event_hour"].dt.date.nunique(),
        "single_coin": scopes.get("single_coin", 0),
        "multi_coin": scopes.get("multi_coin", 0),
        "median_return_1h": statistics.median(events["return_1h"].tolist()),
    }


PERCENT_COLUMNS = (
    "return_1h",
    "return_4h",
    "return_before",
    "return_after",
    "open_interest_change_before",
    "open_interest_change_after",
)
BASIS_POINT_COLUMNS = ("funding_rate_8h_before",)


def events_table(events: pd.DataFrame) -> pd.DataFrame:
    """pump_events for display: fractions in percent, funding in basis points, and the
    scope under the label the filter uses."""
    table = events.copy()
    table["scope_label"] = table["event_scope"].map(SCOPES.__getitem__)
    table[list(PERCENT_COLUMNS)] = table[list(PERCENT_COLUMNS)] * 100
    table[list(BASIS_POINT_COLUMNS)] = table[list(BASIS_POINT_COLUMNS)] * 10_000
    return table


def event_window(con: duckdb.DuckDBPyConnection, pump_event_id: str) -> pd.DataFrame:
    """One event's hours, -24 ... +24 around the pump hour (offset 0)."""
    return frame(
        con,
        """
        SELECT
            offset_hours, open_time, return_since_start, volume_ratio, taker_buy_share,
            funding_premium_8h, open_interest_change_since_start
        FROM fct_pump_event_window
        WHERE pump_event_id = $id
        ORDER BY offset_hours
        """,
        {"id": pump_event_id},
    )


# --- profile -----------------------------------------------------------------------------


@dataclass(frozen=True)
class Metric:
    label: str
    unit: str  # "%" (a fraction shown in percent), "x" (a ratio), "bp" (basis points)
    description: str

    @property
    def scale(self) -> float:
        return {"%": 100.0, "x": 1.0, "bp": 10_000.0}[self.unit]


METRICS: dict[str, Metric] = {
    "return_since_start": Metric(
        "Price vs start", "%", "Close against the close of hour -1, the start of the event."
    ),
    "return_since_anchor_close": Metric(
        "Price after the pump hour",
        "%",
        "Close against the close of hour 0: what buying at the end of the pump hour saw.",
    ),
    "volume_ratio": Metric(
        "Volume vs 7-day baseline", "x", "Quote volume of the hour over the 7 days before."
    ),
    "taker_buy_share": Metric(
        "Taker buy share", "%", "Share of the hour's volume bought by takers (about 50 %)."
    ),
    "funding_premium_8h": Metric(
        "Funding premium",
        "bp",
        "Funding per 8 h minus the contract's interest rate; below zero shorts pay longs.",
    ),
    "open_interest_change_since_start": Metric(
        "Open interest vs start", "%", "Open interest in coins against hour -1 (~30 days)."
    ),
    "top_position_ratio_change_since_start": Metric(
        "Top traders' long/short (positions) vs start",
        "%",
        "Long/short ratio of the top traders' positions against hour -1 (~30 days).",
    ),
    "top_account_ratio_change_since_start": Metric(
        "Top traders' long/short (accounts) vs start",
        "%",
        "Long/short ratio of the top traders' accounts against hour -1 (~30 days).",
    ),
    "global_account_ratio_change_since_start": Metric(
        "All accounts' long/short vs start",
        "%",
        "Long/short ratio of all accounts against hour -1 (~30 days).",
    ),
}


def metric_info(metric: str) -> Metric:
    """A metric the dashboard does not know yet is shown as it is, not hidden."""
    return METRICS.get(metric, Metric(metric, "x", "No description yet."))


def ordered(found: list[str], known: list[str]) -> list[str]:
    """Known values in their order, then any others alphabetically."""
    return [k for k in known if k in found] + sorted(set(found) - set(known))


def profile_options(con: duckdb.DuckDBPyConnection) -> tuple[list[str], list[str]]:
    scopes = [
        r[0] for r in con.execute("SELECT DISTINCT event_scope FROM mart_pump_profile").fetchall()
    ]
    metrics = [
        r[0] for r in con.execute("SELECT DISTINCT metric FROM mart_pump_profile").fetchall()
    ]
    return ordered(scopes, list(SCOPES)), ordered(metrics, list(METRICS))


def pump_profile(con: duckdb.DuckDBPyConnection, scope: str, metric: str) -> pd.DataFrame:
    """Event median and quartiles against the control median, by hour offset."""
    return frame(
        con,
        """
        SELECT
            offset_hours, events, event_median, event_p25, event_p75,
            control_hours, control_median, median_excess
        FROM mart_pump_profile
        WHERE event_scope = $scope AND metric = $metric
        ORDER BY offset_hours
        """,
        {"scope": scope, "metric": metric},
    )


PROFILE_VALUES = ("event_median", "event_p25", "event_p75", "control_median", "median_excess")


def scaled_profile(profile: pd.DataFrame, metric: str) -> pd.DataFrame:
    """pump_profile in the metric's display unit (percent, basis points or as is)."""
    table = profile.copy()
    table[list(PROFILE_VALUES)] = table[list(PROFILE_VALUES)] * metric_info(metric).scale
    return table


HIGHLIGHT_OFFSETS = (-6, -3, -1, 0, 1, 4, 24)


def profile_highlights(
    profile: pd.DataFrame, offsets: tuple[int, ...] = HIGHLIGHT_OFFSETS
) -> pd.DataFrame:
    """The profile at a few telling offsets: the run-up, the pump hour, the aftermath."""
    return profile.loc[profile["offset_hours"].isin(offsets)].reset_index(drop=True)


# --- data quality ------------------------------------------------------------------------

STATUS_ORDER = ("error", "fail", "warn", "skipped", "pass")

# A full build checked at least this share of the most checks any build had run so far.
# Selective runs (mutation tests, one model) are builds too, with a handful of checks;
# their numbers must not read as the pipeline's. Needs no command text, so it holds for
# any engine; the share leaves room for checks added to the project over time.
FULL_BUILD_SHARE = 0.5
FULL_BUILDS = """
    builds AS (
        SELECT invocation_id, run_started_at, count(*) AS checks
        FROM dq_check_results
        WHERE dbt_command = 'build'
        GROUP BY invocation_id, run_started_at
    ),
    full_builds AS (
        SELECT invocation_id, run_started_at
        FROM builds
        QUALIFY checks >= $full_share * max(checks) OVER (
            ORDER BY run_started_at, invocation_id ROWS UNBOUNDED PRECEDING
        )
    ),
    latest_full_build AS (
        SELECT invocation_id, run_started_at
        FROM full_builds
        ORDER BY run_started_at DESC, invocation_id DESC
        LIMIT 1
    )
"""


def latest_build(con: duckdb.DuckDBPyConnection) -> dict[str, Any] | None:
    """Status counts of the newest full dbt build's checks (data and unit tests)."""
    row = first_row(
        con,
        f"""
        WITH {FULL_BUILDS}
        SELECT
            latest_full_build.run_started_at,
            count(*),
            count_if(status = 'pass'),
            count_if(status = 'warn'),
            count_if(status IN ('fail', 'error')),
            count_if(status = 'skipped')
        FROM dq_check_results
        INNER JOIN latest_full_build USING (invocation_id)
        GROUP BY latest_full_build.run_started_at
        """,
        {"full_share": FULL_BUILD_SHARE},
    )
    if row is None:
        return None
    keys = ("run_started_at", "checks", "passed", "warned", "failed", "skipped")
    return dict(zip(keys, row, strict=True))


CHECK_LABEL = """
    CASE
        WHEN severity = 'warn' AND failures_relation IS NOT NULL
            THEN trim(split_part(failures_relation, '.', -1), '"')
        ELSE check_name
    END
"""  # warn tests store findings under a readable alias (dq_volume_anomaly); DuckDB
# quotes the parts of the relation ("crypto_dwh"."dq"."dq_volume_anomaly"), Snowflake did not


def check_status(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """The latest result of every check (is_latest of the DQ view), problems first.

    The export runs right after a full build and its dbt test, so these are that build's
    results. in_latest_build is false for a check the newest full build did not run: the
    DQ log reconciliation (dbt test), or a check no longer in the project.
    """
    return frame(
        con,
        f"""
        WITH {FULL_BUILDS}
        SELECT
            {CHECK_LABEL} AS check_label,
            check_type, model_name, severity, status, failures, failures_relation,
            logged_at,
            invocation_id IN (SELECT invocation_id FROM latest_full_build) AS in_latest_build
        FROM dq_check_results
        WHERE is_latest
        ORDER BY
            list_position($order, status) NULLS LAST,
            severity = 'warn' DESC,
            failures DESC NULLS LAST,
            check_label
        """,
        {"order": list(STATUS_ORDER), "full_share": FULL_BUILD_SHARE},
    )


def warn_history(con: duckdb.DuckDBPyConnection, only_with_findings: bool = True) -> pd.DataFrame:
    """Findings of each warn check per full dbt build over time.

    Full builds only: a dbt test run repeats a build's numbers, and a selective run
    (a mutation test) would draw a spike the pipeline never saw.
    """
    return frame(
        con,
        f"""
        WITH {FULL_BUILDS},
        history AS (
            SELECT
                run_started_at, {CHECK_LABEL} AS check_label, status, failures
            FROM dq_check_results
            WHERE severity = 'warn' AND check_type <> 'unit' AND status IN ('pass', 'warn')
                AND invocation_id IN (SELECT invocation_id FROM full_builds)
        )
        SELECT run_started_at, check_label, status, failures
        FROM history
        WHERE NOT $only OR check_label IN (
            SELECT check_label FROM history GROUP BY check_label HAVING max(failures) > 0
        )
        ORDER BY check_label, run_started_at
        """,
        {"only": only_with_findings, "full_share": FULL_BUILD_SHARE},
    )
