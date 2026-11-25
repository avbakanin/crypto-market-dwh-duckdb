"""Extract new hourly klines for whitelisted symbols from Binance into a new batch directory.

Run from repo root: python -m ingestion.extract_klines
Incremental: each symbol starts from its high-water mark (the newest candle already
extracted, see batches.high_water_marks), re-reading OVERLAP_CANDLES; a symbol with no
mark gets the full HISTORY_DAYS window. Writes data/raw/klines_1h/<batch_id>/<SYMBOL>.json
and, last, _manifest.json (see batches.py). Nothing new -> no batch.
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
from ingestion.clients.binance import BinanceClient
from ingestion.extract_cli import exit_code, parse_extract_args
from ingestion.settings import LANDING_ZONE_DIR

Kline = list[Any]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SYMBOLS_PATH = PROJECT_ROOT / "config" / "symbols.csv"

HOUR_MS = 60 * 60 * 1000
DAY_MS = 24 * HOUR_MS
# Fixed-length Binance intervals only: "1M" (calendar month) has no constant length.
INTERVAL_MS_BY_NAME = {"1h": HOUR_MS, "1d": DAY_MS}

INTERVAL = "1h"
INTERVAL_MS = INTERVAL_MS_BY_NAME[INTERVAL]  # unsupported INTERVAL fails at import, not mid-run
OUTPUT_DIR = LANDING_ZONE_DIR / f"klines_{INTERVAL}"
PAGE_LIMIT = 1000
HISTORY_DAYS = 365
OVERLAP_CANDLES = 1  # re-read the newest extracted candle: proves the seam, heals a fast clock
PAUSE_S = 0.2  # politeness pause between pages


class KlinesSource(Protocol):
    """Anything that serves klines like BinanceClient.klines: the real client or a test fake."""

    def klines(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[Kline]: ...


def load_symbols(path: Path = SYMBOLS_PATH) -> list[str]:
    """Read whitelisted symbols from config/symbols.csv."""
    with path.open(encoding="utf-8", newline="") as f:
        return [row["symbol"] for row in csv.DictReader(f)]


def fetch_history(client: KlinesSource, symbol: str, start_ms: int, now_ms: int) -> list[Kline]:
    """Page through klines from start_ms until now_ms."""
    all_klines: list[Kline] = []
    while start_ms < now_ms:
        batch = client.klines(symbol, INTERVAL, start_time=start_ms, limit=PAGE_LIMIT)
        if not batch:
            break
        all_klines.extend(batch)
        start_ms = batch[-1][0] + INTERVAL_MS  # next candle after the last one received
        if len(batch) < PAGE_LIMIT:
            break  # reached the most recent candle
        time.sleep(PAUSE_S)
    return all_klines


def last_closed_open_ms(now_ms: int) -> int:
    """open_time of the newest candle closed before now_ms (open + INTERVAL_MS - 1 < now_ms)."""
    return (now_ms - INTERVAL_MS) // INTERVAL_MS * INTERVAL_MS


def start_ms_for(mark: int | None, now_ms: int) -> int:
    """First open_time to request: the history window for a new symbol, else the newest
    OVERLAP_CANDLES already extracted; never earlier than the history window."""
    window_start = now_ms - HISTORY_DAYS * DAY_MS
    if mark is None:
        return window_start
    return max(window_start, mark - (OVERLAP_CANDLES - 1) * INTERVAL_MS)


def expected_candles(start_ms: int, now_ms: int) -> int:
    """Closed candles on the interval grid with open_time >= start_ms (DQ: compare with rows)."""
    first = -(-start_ms // INTERVAL_MS) * INTERVAL_MS  # first grid point at or after start_ms
    return max(0, (last_closed_open_ms(now_ms) - first) // INTERVAL_MS + 1)


def drop_unclosed(klines: list[Kline], now_ms: int) -> list[Kline]:
    """Keep only candles closed before the run's as-of moment (DQ note #2)."""
    return [k for k in klines if k[6] < now_ms]


def check_klines(klines: list[Kline]) -> dict[str, int]:
    """DQ checks: row count, duplicate open_time, gaps in the interval grid."""
    open_times = [k[0] for k in klines]
    unique = sorted(set(open_times))
    gaps = sum(1 for prev, curr in pairwise(unique) if curr - prev != INTERVAL_MS)
    return {
        "rows": len(klines),
        "duplicates": len(open_times) - len(unique),
        "gaps": gaps,
    }


def ms_to_utc(ms: int) -> datetime:
    """Convert Unix milliseconds to timezone-aware UTC datetime."""
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def extract_batch(
    client: KlinesSource, symbols: list[str], now_ms: int, output_dir: Path = OUTPUT_DIR
) -> Path | None:
    """Extract what is new since each symbol's high-water mark into a new batch directory.

    Returns the batch directory, or None when no symbol has a new closed candle: an empty
    batch adds no rows to RAW, so the loader would never see it as loaded and retry it forever.
    """
    writer = BatchWriter(output_dir, now_ms, INTERVAL)  # refuses an existing batch up front
    marks = high_water_marks(output_dir)
    newest_closed = last_closed_open_ms(now_ms)

    no_new: list[str] = []
    for i, symbol in enumerate(symbols, start=1):
        prefix = f"[{i:>2}/{len(symbols)}] {symbol:<10}"
        mark = marks.get(symbol)
        if mark is not None and mark >= newest_closed:
            print(f"{prefix} up to date")  # no new closed candle can exist: no request
            continue
        start_ms = start_ms_for(mark, now_ms)
        new_from = start_ms if mark is None else max(start_ms, mark + INTERVAL_MS)
        klines = drop_unclosed(fetch_history(client, symbol, start_ms, now_ms), now_ms)
        if not any(k[0] >= new_from for k in klines):
            # Trading halted or delisted? The symbol stays out of the batch.
            no_new.append(symbol)
            expected_new = expected_candles(new_from, now_ms)
            print(f"{prefix} WARNING: no new candles, expected {expected_new}")
            continue

        stats = check_klines(klines)
        stats["expected"] = expected_candles(start_ms, now_ms)
        stats["last_open_ms"] = max(k[0] for k in klines)  # the next run's high-water mark
        writer.add(symbol, klines, stats)
        print(
            f"{prefix} from {ms_to_utc(start_ms):%Y-%m-%d %H:%M} rows={stats['rows']:>5} "
            f"expected={stats['expected']:>5} dup={stats['duplicates']} gaps={stats['gaps']}"
        )

    batch_dir = writer.finish()  # the manifest, last: a batch without one is incomplete
    if batch_dir is None:
        print(f"\nNothing new as of {ms_to_utc(now_ms):%Y-%m-%d %H:%M} UTC: no batch written")
        return None

    results = writer.symbols
    with_issues = [
        s
        for s, st in results.items()
        if st["duplicates"] or st["gaps"] or st["rows"] - st["duplicates"] != st["expected"]
    ]
    print(f"\nDone: {len(results)} symbols, with DQ issues: {len(with_issues)} {with_issues or ''}")
    if no_new:
        print(f"No new candles (left out of the batch): {no_new}")
    print(f"Batch: {batch_dir}")
    return batch_dir


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_extract_args("Extract new closed 1h spot klines.", argv)
    symbols = load_symbols()
    now_ms = int(time.time() * 1000)  # single as-of moment for the whole run
    print(
        f"Extracting {len(symbols)} symbols, {INTERVAL}, "
        f"as of {ms_to_utc(now_ms):%Y-%m-%d %H:%M} UTC"
    )
    with BinanceClient() as client:
        batch_dir = extract_batch(client, symbols, now_ms)
    return exit_code(batch_dir, args.nothing_new_exit_code)


if __name__ == "__main__":
    sys.exit(main())
