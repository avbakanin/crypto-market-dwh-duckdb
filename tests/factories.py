"""Test data factories: synthetic Binance klines, landing-zone batches and mart exports,
no network needed."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.batches import write_manifest
from ingestion.extract_klines import DAY_MS, HOUR_MS, INTERVAL_MS, Kline

START_MS = 1_735_689_600_000  # 2025-01-01 00:00:00 UTC, aligned to the hour


def make_kline(open_ms: int) -> Kline:
    """Build one kline in Binance's 12-field format; prices are strings like the real API."""
    close_ms = open_ms + INTERVAL_MS - 1
    return [
        open_ms,  # 0 open_time
        "100.00000000",  # 1 open
        "101.00000000",  # 2 high
        "99.00000000",  # 3 low
        "100.50000000",  # 4 close
        "10.00000000",  # 5 volume
        close_ms,  # 6 close_time
        "1005.00000000",  # 7 quote_asset_volume
        42,  # 8 number_of_trades
        "5.00000000",  # 9 taker_buy_base_volume
        "502.50000000",  # 10 taker_buy_quote_volume
        "0",  # 11 ignore
    ]


def make_klines(count: int, start_ms: int = START_MS) -> list[Kline]:
    """Build `count` consecutive hourly klines with no duplicates and no gaps."""
    return [make_kline(start_ms + i * INTERVAL_MS) for i in range(count)]


def make_symbol_info(
    base: str, quote: str = "USDT", status: str = "TRADING", spot: bool = True
) -> dict[str, Any]:
    """Build one entry of exchangeInfo["symbols"] with the fields select_symbols reads."""
    return {
        "symbol": base + quote,
        "status": status,
        "baseAsset": base,
        "quoteAsset": quote,
        "isSpotTradingAllowed": spot,
    }


def make_contract(
    base: str,
    symbol: str | None = None,
    contract_type: str = "PERPETUAL",
    status: str = "TRADING",
    quote: str = "USDT",
) -> dict[str, Any]:
    """Build one entry of the futures exchangeInfo["symbols"] (live field names and values)."""
    return {
        "symbol": symbol or base + quote,
        "pair": base + quote,
        "contractType": contract_type,
        "status": status,
        "baseAsset": base,
        "quoteAsset": quote,
        "marginAsset": quote,
    }


def make_premium_index(symbol: str, interest_rate: str = "0.00010000") -> dict[str, Any]:
    """One entry of /fapi/v1/premiumIndex (no symbol: a list) in the live shape.

    lastFundingRate differs from interestRate on purpose: copying the wrong field fails.
    """
    return {
        "symbol": symbol,
        "markPrice": "100.00000000",
        "indexPrice": "100.01000000",
        "estimatedSettlePrice": "100.00500000",
        "lastFundingRate": "0.00003810",
        "interestRate": interest_rate,
        "nextFundingTime": 1_735_718_400_000,
        "time": 1_735_689_600_000,
    }


def make_funding(symbol: str, funding_ms: int, rate: str = "0.00010000") -> dict[str, Any]:
    """One record of /fapi/v1/fundingRate in the live shape: numbers as strings."""
    return {
        "symbol": symbol,
        "fundingTime": funding_ms,
        "fundingRate": rate,
        "markPrice": "100.00000000",
        "rateType": "Regular",
    }


def make_fundings(
    symbol: str, first_ms: int, count: int, interval_hours: int = 8
) -> list[dict[str, Any]]:
    """`count` consecutive settlements from first_ms; each lands 0-26 ms after its hour, as live."""
    step = interval_hours * HOUR_MS
    return [make_funding(symbol, first_ms + i * step + i * 7 % 27) for i in range(count)]


def make_open_interest(symbol: str, timestamp_ms: int) -> dict[str, Any]:
    """One point of /futures/data/openInterestHist in the live shape: numbers as strings."""
    return {
        "symbol": symbol,
        "sumOpenInterest": "96000.00000000",
        "sumOpenInterestValue": "5600000000.00000000",
        "CMCCirculatingSupply": "20114434.00000000",
        "timestamp": timestamp_ms,
    }


def make_open_interests(symbol: str, first_ms: int, count: int) -> list[dict[str, Any]]:
    """`count` consecutive hourly points from first_ms (on the hour, as live)."""
    return [make_open_interest(symbol, first_ms + i * HOUR_MS) for i in range(count)]


def make_long_short_ratio(
    symbol: str, timestamp_ms: int, long_account: str = "0.6000"
) -> dict[str, Any]:
    """One point of a /futures/data/ long/short ratio endpoint in the live shape: shares
    rounded to 4 decimals, numbers as strings."""
    long_share = float(long_account)
    return {
        "symbol": symbol,
        "longAccount": long_account,
        "longShortRatio": f"{long_share / (1 - long_share):.4f}",
        "shortAccount": f"{1 - long_share:.4f}",
        "timestamp": timestamp_ms,
    }


def make_long_short_ratios(
    symbol: str, first_ms: int, count: int, long_account: str = "0.6000"
) -> list[dict[str, Any]]:
    """`count` consecutive hourly points from first_ms (on the hour, as live)."""
    return [
        make_long_short_ratio(symbol, first_ms + i * HOUR_MS, long_account) for i in range(count)
    ]


def make_daily_klines(
    first_open_ms: int,
    last_open_ms: int,
    close: str = "100.00000000",
    quote_volume: str = "20000000.00000000",
) -> list[Kline]:
    """Build consecutive daily klines from first_open_ms to last_open_ms inclusive.

    Candles are flat (open = high = low = close): select_symbols reads only close and quote volume.
    """
    return [
        [
            open_ms,
            close,
            close,
            close,
            close,
            "1.00000000",
            open_ms + DAY_MS - 1,
            quote_volume,
            42,
            "0.50000000",
            "0",
            "0",
        ]
        for open_ms in range(first_open_ms, last_open_ms + 1, DAY_MS)
    ]


def make_marked_batch(
    root: Path, batch_id: str, last_open: dict[str, int], key: str = "last_open_ms"
) -> Path:
    """A complete batch whose manifest records the newest record time per symbol under `key`
    (klines: last_open_ms, other datasets: last_ms); no data files.

    Enough for high_water_marks, which reads the manifest: seeds what earlier runs extracted.
    """
    batch_dir = root / batch_id
    batch_dir.mkdir(parents=True)
    write_manifest(
        batch_dir,
        {
            "batch_id": batch_id,
            "as_of_utc": "2025-01-01T00:00:00+00:00",
            "interval": "1h",
            "symbols": {
                s: {"rows": 1, "duplicates": 0, "gaps": 0, key: ms} for s, ms in last_open.items()
            },
        },
    )
    return batch_dir


# --- mart exports (data/marts/<export_id>/), as ingestion/export_marts.py writes them -------

EXPORTED_AT = datetime(2026, 11, 24, 9, 10, 28, tzinfo=UTC)
EXPORT_ID = "20261124T091028Z"

_TS = pa.timestamp("us", tz="UTC")
_DEC = pa.decimal128(38, 12)


def _decimals(*names: str) -> list[tuple[str, pa.DataType]]:
    return [(name, _DEC) for name in names]


# The columns the dashboard reads, with the types the export writes (decimal, UTC time).
MART_SCHEMAS: dict[str, pa.Schema] = {
    "dim_symbol": pa.schema([("symbol", pa.string()), ("has_perpetual", pa.bool_())]),
    "fct_pump_events": pa.schema(
        [
            ("pump_event_id", pa.string()),
            ("symbol", pa.string()),
            ("event_hour", _TS),
            ("event_scope", pa.string()),
            ("market_breadth", pa.int64()),
            ("episode_hours", pa.int64()),
            ("overlaps_previous_event", pa.bool_()),
        ]
        + _decimals(
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
        )
    ),
    "fct_pump_event_window": pa.schema(
        [("pump_event_id", pa.string()), ("offset_hours", pa.int64()), ("open_time", _TS)]
        + _decimals(
            "return_since_start",
            "volume_ratio",
            "taker_buy_share",
            "funding_premium_8h",
            "open_interest_change_since_start",
        )
    ),
    "mart_pump_profile": pa.schema(
        [
            ("event_scope", pa.string()),
            ("offset_hours", pa.int64()),
            ("metric", pa.string()),
            ("events", pa.int64()),
            ("control_hours", pa.int64()),
        ]
        + _decimals("event_median", "event_p25", "event_p75", "control_median", "median_excess")
    ),
    "fct_pump_event_excess": pa.schema(
        [
            ("pump_event_id", pa.string()),
            ("offset_hours", pa.int64()),
            ("metric", pa.string()),
            ("control_hours", pa.int64()),
        ]
        + _decimals(
            "value",
            "control_median",
            "control_share_above_zero",
            "control_share_below_zero",
            "excess",
        )
    ),
    "mart_pump_signal_check": pa.schema(
        [
            ("rule", pa.string()),
            ("signal_date", pa.date32()),
            ("signal_hours", pa.int64()),
            ("followed_by_pump", pa.int64()),
        ]
    ),
    "dq_check_results": pa.schema(
        [
            ("invocation_id", pa.string()),
            ("run_started_at", _TS),
            ("dbt_command", pa.string()),
            ("check_unique_id", pa.string()),
            ("check_name", pa.string()),
            ("check_type", pa.string()),
            ("model_name", pa.string()),
            ("severity", pa.string()),
            ("status", pa.string()),
            ("failures", pa.int64()),
            ("failures_relation", pa.string()),
            ("logged_at", _TS),
            ("is_latest", pa.bool_()),
        ]
    ),
    "dq_data_coverage": pa.schema(
        [
            ("dataset", pa.string()),
            ("row_count", pa.int64()),
            ("symbol_count", pa.int64()),
            ("data_from", _TS),
            ("data_to", _TS),
            ("latest_batch_id", pa.string()),
        ]
    ),
    "fct_signal_log": pa.schema(
        [
            ("decision_date", pa.date32()),
            ("version", pa.string()),
            ("status", pa.string()),
            ("reason", pa.string()),
            ("symbol", pa.string()),
            ("side", pa.string()),
            ("hedge_symbol", pa.string()),
            ("hedge_side", pa.string()),
            ("planned_exit_time", _TS),
            ("outcome_status", pa.string()),
            ("exit_reason", pa.string()),
        ]
        + _decimals("hedge_ratio", "stop_loss", "net_return", "mae", "excess_return")
    ),
    "mart_signal_performance": pa.schema(
        [
            ("version", pa.string()),
            ("days_logged", pa.int64()),
            ("days_missed", pa.int64()),
            ("trades", pa.int64()),
            ("open_trades", pa.int64()),
            ("closed_trades", pa.int64()),
            ("stopped_trades", pa.int64()),
            ("hit_rate", pa.float64()),
            ("mean_net", pa.float64()),
            ("ci_low", pa.float64()),
            ("ci_high", pa.float64()),
            ("mean_excess", pa.float64()),
            ("survive_3x_share", pa.float64()),
            ("worst_net", pa.float64()),
            ("days_running", pa.int64()),
            ("criteria_met", pa.bool_()),
        ]
    ),
}


def utc(text: str) -> datetime:
    """'2026-03-02 10:00' as an aware UTC datetime."""
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def _event(
    event_id: str, symbol: str, hour: str, scope: str, breadth: int, return_1h: float, **rest: Any
) -> dict[str, Any]:
    return {
        "pump_event_id": event_id,
        "symbol": symbol,
        "event_hour": utc(hour),
        "event_scope": scope,
        "market_breadth": breadth,
        "episode_hours": 1,
        "overlaps_previous_event": False,
        "return_1h": return_1h,
        **rest,
    }


def _check(
    invocation: str, started: str, command: str, name: str, status: str, **rest: Any
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "invocation_id": invocation,
        "run_started_at": utc(started),
        "dbt_command": command,
        "check_unique_id": f"test.crypto_market_dwh.{name}",
        "check_name": name,
        "check_type": "generic",
        "model_name": "stg_binance__klines_1h",
        "severity": "error",
        "status": status,
        "failures": 0,
        "failures_relation": f'"crypto_dwh"."dq"."{name}"',
        "logged_at": utc(started),
        "is_latest": False,
    }
    row.update(rest)
    return row


def _profile_row(
    scope: str, offset: int, metric: str, events: int, median: float, control: float
) -> dict[str, Any]:
    return {
        "event_scope": scope,
        "offset_hours": offset,
        "metric": metric,
        "events": events,
        "event_median": median,
        "event_p25": median - 0.5,
        "event_p75": median + 0.5,
        "control_hours": events * 30,
        "control_median": control,
        "median_excess": median - control,
    }


def make_mart_rows() -> dict[str, list[dict[str, Any]]]:
    """A small export: 3 symbols, 4 pump events, 3 profile metrics, 2 builds of checks.

    Events (UTC): e1 AAAUSDT 2026-01-10 05:00 single coin +6 %; e2 AAAUSDT and e3 BBBUSDT
    2026-03-02 10:00, one market move (+12 %, +8 %); e4 BBBUSDT 2026-03-03 12:00 single
    coin +5.1 %, overlapping e3. CCCUSDT has no event and no perpetual.
    Profile: offsets -1, 0, 1, 24 stored out of order; brand_new_metric (unknown to the
    dashboard) only in scope all. Checks: build b1 (2026-11-23 21:08), test t1 (21:10,
    the DQ log reconciliation), build b2 (2026-11-24 09:08, the newest): volume anomaly
    24 -> 25 findings, missing funding 4 -> 4, missing klines hours 0; old_check only in
    b1; accepted_values fails in b2; p1 (2026-11-24 08:14) is a selective build of one
    check (a mutation test) with 30 findings. Coverage: klines end 08:00, open interest 09:00.
    """
    volume = {
        "severity": "warn",
        "model_name": "int_binance__hourly_returns",
        "failures_relation": '"crypto_dwh"."dq"."dq_volume_anomaly"',
    }
    funding = {
        "severity": "warn",
        "model_name": "stg_binance__funding_rate",
        "failures_relation": '"crypto_dwh"."dq"."dq_funding_missing_settlements"',
    }
    grid = {"severity": "warn", "failures_relation": '"crypto_dwh"."dq"."dq_klines_missing_hours"'}
    b1 = ("b1", "2026-11-23 21:08", "build")
    b2 = ("b2", "2026-11-24 09:08", "build")
    window = [
        ("e1", 1, "2026-01-10 06:00", {"return_since_start": 0.07, "volume_ratio": 3.0}),
        ("e1", -1, "2026-01-10 04:00", {"return_since_start": 0, "funding_premium_8h": -0.0001}),
        ("e1", 0, "2026-01-10 05:00", {"return_since_start": 0.06, "taker_buy_share": 0.6}),
        ("e2", 0, "2026-03-02 10:00", {"return_since_start": 0.12, "volume_ratio": 5.0}),
    ]
    coverage_ends = {
        "funding_rate": "2026-11-24 08:00",
        "global_account_ratio_1h": "2026-11-24 09:00",
        "klines_1h": "2026-11-24 08:00",
        "open_interest_1h": "2026-11-24 09:00",
        "top_account_ratio_1h": "2026-11-24 09:00",
        "top_position_ratio_1h": "2026-11-24 09:00",
    }
    return {
        "dim_symbol": [
            {"symbol": "AAAUSDT", "has_perpetual": True},
            {"symbol": "BBBUSDT", "has_perpetual": True},
            {"symbol": "CCCUSDT", "has_perpetual": False},
        ],
        "fct_pump_events": [
            _event("e1", "AAAUSDT", "2026-01-10 05:00", "single_coin", 1, 0.06, return_after=0.01),
            _event("e2", "AAAUSDT", "2026-03-02 10:00", "multi_coin", 2, 0.12),
            _event("e3", "BBBUSDT", "2026-03-02 10:00", "multi_coin", 2, 0.08),
            _event(
                "e4",
                "BBBUSDT",
                "2026-03-03 12:00",
                "single_coin",
                1,
                0.051,
                overlaps_previous_event=True,
            ),
        ],
        "fct_pump_event_window": [
            {"pump_event_id": e, "offset_hours": o, "open_time": utc(t), **values}
            for e, o, t, values in window
        ],
        "mart_pump_profile": [
            _profile_row(scope, offset, metric, events, median, control)
            for scope, events in (("all", 4), ("single_coin", 2), ("multi_coin", 2))
            for metric, control in (
                ("brand_new_metric", 0.0),
                ("volume_ratio", 0.7),
                ("funding_premium_8h", -0.00001),
            )
            for offset, median in ((1, 4.1), (-1, 1.56), (0, 4.5), (24, 1.1))
            if metric != "brand_new_metric" or scope == "all"
        ],
        "dq_check_results": [
            _check(*b1, "not_null_open_price", "pass"),
            _check(*b1, "volume_anomaly_hashed_name", "warn", failures=24, **volume),
            _check(*b1, "funding_missing_hashed_name", "warn", failures=4, **funding),
            _check(*b1, "klines_grid_hashed_name", "pass", **grid),
            _check(*b1, "old_check", "pass", is_latest=True),
            _check(
                "t1",
                "2026-11-23 21:10",
                "test",
                "assert_dq_log_matches",
                "pass",
                check_type="singular",
                is_latest=True,
            ),
            _check(
                "p1",
                "2026-11-24 08:14",
                "build",
                "volume_anomaly_hashed_name",
                "warn",
                failures=30,
                **volume,
            ),
            _check(*b2, "not_null_open_price", "pass", is_latest=True),
            _check(
                *b2, "volume_anomaly_hashed_name", "warn", failures=25, is_latest=True, **volume
            ),
            _check(
                *b2, "funding_missing_hashed_name", "warn", failures=4, is_latest=True, **funding
            ),
            _check(*b2, "klines_grid_hashed_name", "pass", is_latest=True, **grid),
            _check(*b2, "accepted_values_interval", "fail", failures=3, is_latest=True),
            _check(
                *b2,
                "klines_unit_test",
                "pass",
                check_type="unit",
                is_latest=True,
                failures_relation=None,
            ),
        ],
        "dq_data_coverage": [
            {
                "dataset": dataset,
                "row_count": 100,
                "symbol_count": 23,
                "data_from": utc("2025-11-21 09:00"),
                "data_to": utc(end),
                "latest_batch_id": "20261124T090711Z",
            }
            for dataset, end in coverage_ends.items()
        ],
    }


def _arrow_value(value: Any, field: pa.Field) -> Any:
    if value is not None and pa.types.is_decimal(field.type):
        return Decimal(str(round(value, field.type.scale)))  # floats like 4.1 - 0.5
    return value


def write_mart_export(
    root: Path,
    export_id: str = EXPORT_ID,
    exported_at: datetime = EXPORTED_AT,
    rows: dict[str, list[dict[str, Any]]] | None = None,
) -> Path:
    """An export directory as export_marts writes it: one Parquet file per table, then the
    manifest with each table's rows and columns. Missing row keys become NULL; a table
    left out of `rows` is left out of the export."""
    rows = make_mart_rows() if rows is None else rows
    export_dir = root / export_id
    export_dir.mkdir(parents=True)
    tables: dict[str, Any] = {}
    for name, records in rows.items():
        schema = MART_SCHEMAS[name]
        table = pa.Table.from_pylist(
            [{f.name: _arrow_value(r.get(f.name), f) for f in schema} for r in records],
            schema=schema,
        )
        pq.write_table(table, export_dir / f"{name}.parquet")
        tables[name] = {
            "relation": f"{'dq' if name.startswith('dq_') else 'marts'}.{name}",
            "rows": table.num_rows,
            "columns": {f.name: str(f.type) for f in schema},
        }
    manifest = {
        "export_id": export_id,
        "exported_at_utc": exported_at.isoformat(),
        "source": "duckdb",
        "database": "crypto_dwh.duckdb",
        "tables": tables,
    }
    (export_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return export_dir
