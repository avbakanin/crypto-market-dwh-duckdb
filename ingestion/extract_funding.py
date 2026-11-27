"""Extract funding rate history of the whitelisted perpetuals into a new batch directory.

Run from repo root: python -m ingestion.extract_funding
Contracts come from config/perpetuals.csv (see map_perpetuals). Incremental like klines:
each contract starts from its high-water mark, re-reading that record (startTime is
inclusive); a contract with no mark gets the full HISTORY_DAYS window. Writes
data/raw/funding_rate/<batch_id>/<CONTRACT>.json and, last, _manifest.json (batches.py).

Source traps (verified live): fundingTime lands 0-26 ms after the hour, so the grid key is
the hour; the interval is 8 h or 4 h per contract and changes over time, and the API serves
only the current one (fundingInfo); the platform itself skips settlements now and then.
So `expected` is an estimate: a mismatch is a DQ warning, not an error.
"""

from __future__ import annotations

import csv
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any, Protocol

from ingestion.batches import BatchWriter, high_water_marks
from ingestion.clients.binance import BinanceFuturesClient
from ingestion.extract_cli import exit_code, parse_extract_args
from ingestion.extract_klines import DAY_MS, HOUR_MS
from ingestion.settings import LANDING_ZONE_DIR

Funding = dict[str, Any]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PERPETUALS_PATH = PROJECT_ROOT / "config" / "perpetuals.csv"
OUTPUT_DIR = LANDING_ZONE_DIR / "funding_rate"

HISTORY_DAYS = 365
PAGE_LIMIT = 1000
# fundingInfo lists only contracts whose cap, floor or interval was adjusted; others keep 8 h.
DEFAULT_INTERVAL_HOURS = 8
# A settlement is expected in the files only this long after its time: publication delay.
PUBLISH_LAG_MS = 5 * 60 * 1000
MANIFEST_INTERVAL = "funding"  # per contract: see interval_hours in the manifest stats
PAUSE_S = 0.2  # politeness pause between pages


class FundingSource(Protocol):
    """What extract needs from the futures API: the real client or a test fake."""

    def funding_info(self) -> list[dict[str, Any]]: ...

    def funding_rate(
        self,
        symbol: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[Funding]: ...


def load_contracts(path: Path = PERPETUALS_PATH) -> list[str]:
    """Futures symbols of the whitelisted coins, from config/perpetuals.csv."""
    with path.open(encoding="utf-8", newline="") as f:
        return [row["futures_symbol"] for row in csv.DictReader(f)]


def funding_intervals(info: list[dict[str, Any]], contracts: list[str]) -> dict[str, int]:
    """Current funding interval in hours per contract; DEFAULT_INTERVAL_HOURS if not listed."""
    listed = {row["symbol"]: int(row["fundingIntervalHours"]) for row in info}
    return {c: listed.get(c, DEFAULT_INTERVAL_HOURS) for c in contracts}


def funding_hour(ms: int) -> int:
    """Grid key of a settlement: its hour (fundingTime arrives a few ms after it)."""
    return ms // HOUR_MS * HOUR_MS


def last_settled_ms(now_ms: int, interval_hours: int) -> int:
    """Newest settlement on the grid (aligned to 00:00 UTC) that should be published by now."""
    step = interval_hours * HOUR_MS
    return (now_ms - PUBLISH_LAG_MS) // step * step


def start_ms_for(mark: int | None, now_ms: int, history_days: int = HISTORY_DAYS) -> int:
    """First fundingTime to request: the history window for a new contract, else the mark
    itself (re-read: proves the seam); never earlier than the history window. The window
    starts on the hour: a settlement lands a few ms after it, and expected_settlements
    counts the start's hour."""
    window_start = (now_ms - history_days * DAY_MS) // HOUR_MS * HOUR_MS
    return window_start if mark is None else max(window_start, mark)


def expected_settlements(start_ms: int, now_ms: int, interval_hours: int) -> int:
    """Settlements on the current interval's grid from start_ms's hour to the newest published."""
    step = interval_hours * HOUR_MS
    first = -(-funding_hour(start_ms) // step) * step  # first grid point at or after that hour
    return max(0, (last_settled_ms(now_ms, interval_hours) - first) // step + 1)


def fetch_funding(client: FundingSource, symbol: str, start_ms: int, now_ms: int) -> list[Funding]:
    """Page forward from start_ms (inclusive); keep only records settled by now_ms."""
    records: list[Funding] = []
    while start_ms <= now_ms:
        page = client.funding_rate(symbol, start_time=start_ms, limit=PAGE_LIMIT)
        if not page:
            break
        records.extend(page)
        start_ms = page[-1]["fundingTime"] + 1  # the first page holds the first `limit` records
        if len(page) < PAGE_LIMIT:
            break  # reached the newest record
        time.sleep(PAUSE_S)
    return [r for r in records if r["fundingTime"] <= now_ms]  # as-of: same moment for all


def check_funding(records: list[Funding], interval_hours: int) -> dict[str, int]:
    """DQ checks on the hour grid: row count, duplicate hours, steps other than the interval."""
    hours = [funding_hour(r["fundingTime"]) for r in records]
    unique = sorted(set(hours))
    step = interval_hours * HOUR_MS
    return {
        "rows": len(records),
        "duplicates": len(hours) - len(unique),
        "gaps": sum(1 for prev, curr in pairwise(unique) if curr - prev != step),
    }


def ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def extract_batch(
    client: FundingSource,
    contracts: list[str],
    now_ms: int,
    output_dir: Path = OUTPUT_DIR,
    history_days: int = HISTORY_DAYS,
) -> Path | None:
    """Extract funding records new since each contract's mark into a new batch directory.

    Returns the batch directory, or None when no contract has a new settlement.
    """
    writer = BatchWriter(output_dir, now_ms, MANIFEST_INTERVAL)  # refuses an existing batch
    marks = high_water_marks(output_dir)
    intervals = funding_intervals(client.funding_info(), contracts)

    no_new: list[str] = []
    for i, symbol in enumerate(contracts, start=1):
        prefix = f"[{i:>2}/{len(contracts)}] {symbol:<13}"
        interval = intervals[symbol]
        mark = marks.get(symbol)
        if mark is not None and funding_hour(mark) >= last_settled_ms(now_ms, interval):
            print(f"{prefix} up to date ({interval}h)")  # no settlement since: no request
            continue
        start_ms = start_ms_for(mark, now_ms, history_days)
        records = fetch_funding(client, symbol, start_ms, now_ms)
        new_from = start_ms if mark is None else funding_hour(mark) + HOUR_MS
        if not any(r["fundingTime"] >= new_from for r in records):
            # Settlement skipped, contract halted or delisted? It stays out of the batch.
            no_new.append(symbol)
            expected_new = expected_settlements(new_from, now_ms, interval)
            print(f"{prefix} WARNING: no new settlements, expected {expected_new}")
            continue

        stats = check_funding(records, interval)
        stats["expected"] = expected_settlements(start_ms, now_ms, interval)
        stats["interval_hours"] = interval
        stats["last_ms"] = max(r["fundingTime"] for r in records)  # the next run's mark
        writer.add(symbol, records, stats)
        print(
            f"{prefix} from {ms_to_utc(start_ms):%Y-%m-%d %H:%M} {interval}h "
            f"rows={stats['rows']:>5} expected={stats['expected']:>5} "
            f"dup={stats['duplicates']} gaps={stats['gaps']}"
        )

    batch_dir = writer.finish()  # the manifest, last: a batch without one is incomplete
    if batch_dir is None:
        print(f"\nNothing new as of {ms_to_utc(now_ms):%Y-%m-%d %H:%M} UTC: no batch written")
        return None

    with_issues = [
        s
        for s, st in writer.symbols.items()
        if st["duplicates"] or st["gaps"] or st["rows"] - st["duplicates"] != st["expected"]
    ]
    print(
        f"\nDone: {len(writer.symbols)} contracts, with DQ issues: {len(with_issues)} "
        f"{with_issues or ''}"
    )
    if no_new:
        print(f"No new settlements (left out of the batch): {no_new}")
    print(f"Batch: {batch_dir}")
    return batch_dir


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_extract_args("Extract new USD-M funding rates.", argv)
    contracts = load_contracts()
    now_ms = int(time.time() * 1000)  # single as-of moment for the whole run
    print(
        f"Extracting funding rates of {len(contracts)} contracts "
        f"as of {ms_to_utc(now_ms):%Y-%m-%d %H:%M} UTC"
    )
    with BinanceFuturesClient() as client:
        batch_dir = extract_batch(client, contracts, now_ms)
    return exit_code(batch_dir, args.nothing_new_exit_code)


if __name__ == "__main__":
    sys.exit(main())
