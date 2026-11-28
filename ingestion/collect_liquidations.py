"""Collect USD-M liquidations from Binance's public stream into hourly landing-zone batches.

Run from repo root: python -m ingestion.collect_liquidations   (a long-running process; the
compose service `liquidations` keeps it up). PIPELINE_MODE must be "own".

data.binance.vision publishes no USD-M liquidation dataset (checked 2026-11-28), so the
stream is the only free source: what is not collected is lost. The all-market stream
pushes, per contract, at most the latest liquidation of each 1000 ms - a lower bound of the
liquidated volume, not the full tape (source note 142). Only the /market/ host route
answers; the legacy /ws/ route accepts the connection and stays silent.

Two steps, so that a crash loses at most the unflushed line:
1. Spool: every message, and a connection record (open / alive every HEARTBEAT_S / close),
   is appended as one JSON line to data/spool/um_liquidations/<YYYYMMDDTHH>.jsonl, the
   hour it was received in (UTC). Lines: {"r": received_ms, "m": message} or
   {"r": received_ms, "c": "open" | "alive" | "close"}.
2. Seal: a spool hour that ended SEAL_GRACE_MS ago becomes one immutable batch
   um_liquidations/<hour end>/ (batches.py): <SYMBOL>.json - the messages of a contract as
   received; STREAM.json - the connection records, so that a quiet hour is told from a
   disconnected one (staging counts connected minutes). The spool file is removed after the
   manifest is written.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

from ingestion.batches import MANIFEST_NAME, BatchWriter, batch_id_for
from ingestion.settings import LANDING_ZONE_DIR, OWN, pipeline_mode

STREAM_URL = "wss://fstream.binance.com/market/ws/!forceOrder@arr"
OUTPUT_DIR = LANDING_ZONE_DIR / "um_liquidations"
SPOOL_DIR = LANDING_ZONE_DIR.parent / "spool" / "um_liquidations"
STREAM_FILE = "STREAM"  # the connection records of the hour; never a contract name
HOUR_MS = 3_600_000
HEARTBEAT_S = 20  # every connected minute gets at least two records
SEAL_GRACE_MS = 90_000  # the writer has moved to the next hour by then (heartbeats)
SEAL_CHECK_S = 30
RECONNECT_MAX_S = 60
SPOOL_FORMAT = "%Y%m%dT%H"


def hour_start(ms: int) -> int:
    return ms - ms % HOUR_MS


def spool_name(hour_ms: int) -> str:
    return datetime.fromtimestamp(hour_ms / 1000, tz=UTC).strftime(SPOOL_FORMAT) + ".jsonl"


def spool_hour(path: Path) -> int | None:
    """The hour (ms) a spool file holds; None for any other file."""
    if path.suffix != ".jsonl":
        return None
    try:
        parsed = datetime.strptime(path.stem, SPOOL_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None
    return int(parsed.timestamp() * 1000)


def canonical(record: Any) -> str:
    """The text a record is compared by: the loader's duplicate key is payload::VARCHAR."""
    return json.dumps(record, separators=(",", ":"))


def read_spool(path: Path) -> tuple[dict[str, list[Any]], list[dict[str, Any]], int]:
    """Messages by contract, connection records, and the number of unreadable lines (only a
    torn last line after a crash is expected)."""
    by_symbol: dict[str, list[Any]] = defaultdict(list)
    stream: list[dict[str, Any]] = []
    torn = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            torn += 1
            continue
        if "c" in entry:
            stream.append({"r": entry["r"], "c": entry["c"]})
        else:
            message = entry["m"]
            by_symbol[message["o"]["s"]].append(message)
    return by_symbol, stream, torn


def file_stats(records: list[Any], times: list[int]) -> dict[str, int]:
    return {
        "rows": len(records),
        "duplicates": len(records) - len({canonical(r) for r in records}),
        "first_ms": min(times),
        "last_ms": max(times),
    }


def seal(path: Path, output_dir: Path) -> Path | None:
    """Turn a finished spool hour into its batch, then remove the spool file.

    A batch directory without a manifest (a crash while sealing) was never loaded: it is
    replaced. One with a manifest is complete: the spool file is only removed.
    Returns the batch directory, or None when the hour held nothing (no batch).
    """
    hour_ms = spool_hour(path)
    if hour_ms is None:
        raise ValueError(f"Not a spool file: {path}")
    batch_dir = output_dir / batch_id_for(hour_ms + HOUR_MS)
    if (batch_dir / MANIFEST_NAME).is_file():
        path.unlink()
        return batch_dir
    if batch_dir.exists():
        for leftover in batch_dir.iterdir():
            leftover.unlink()
        batch_dir.rmdir()

    by_symbol, stream, torn = read_spool(path)
    if torn:
        print(f"{path.name}: {torn} unreadable line(s) skipped", file=sys.stderr)
    writer = BatchWriter(output_dir, hour_ms + HOUR_MS, interval="event")
    for symbol in sorted(by_symbol):
        messages = by_symbol[symbol]
        writer.add(symbol, messages, file_stats(messages, [m["o"]["T"] for m in messages]))
    if stream:
        writer.add(STREAM_FILE, stream, file_stats(stream, [s["r"] for s in stream]))
    written = writer.finish()
    path.unlink()
    return written


def seal_finished(spool_dir: Path, output_dir: Path, now_ms: int) -> list[Path]:
    """Seal every spool hour that ended at least SEAL_GRACE_MS ago, oldest first."""
    if not spool_dir.is_dir():
        return []
    done = []
    for path in sorted(spool_dir.iterdir()):
        hour_ms = spool_hour(path)
        if hour_ms is not None and hour_ms + HOUR_MS + SEAL_GRACE_MS <= now_ms:
            batch = seal(path, output_dir)
            if batch is not None:
                done.append(batch)
    return done


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class Spool:
    """Appends lines to the file of the hour they are received in; flushed per line."""

    def __init__(self, spool_dir: Path) -> None:
        self.spool_dir = spool_dir
        self.hour_ms: int | None = None
        self.handle: TextIO | None = None

    def write(self, entry: dict[str, Any]) -> None:
        hour_ms = hour_start(entry["r"])
        if hour_ms != self.hour_ms:
            self.close()
            self.spool_dir.mkdir(parents=True, exist_ok=True)
            self.handle = (self.spool_dir / spool_name(hour_ms)).open("a", encoding="utf-8")
            self.hour_ms = hour_ms
        assert self.handle is not None
        self.handle.write(canonical(entry) + "\n")
        self.handle.flush()

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()
        self.handle, self.hour_ms = None, None


async def heartbeat(spool: Spool) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_S)
        spool.write({"r": now_ms(), "c": "alive"})


async def stream_once(spool: Spool) -> None:
    import websockets  # the collector's own dependency: the batch extracts never need it

    async with websockets.connect(STREAM_URL, open_timeout=15, max_queue=None) as ws:
        spool.write({"r": now_ms(), "c": "open"})
        beat = asyncio.create_task(heartbeat(spool))
        try:
            async for raw in ws:
                message = json.loads(raw)
                if message.get("e") == "forceOrder":
                    spool.write({"r": now_ms(), "m": message})
        finally:
            beat.cancel()


async def stream_forever(spool: Spool) -> None:
    delay = 1
    while True:
        started = time.monotonic()
        try:
            await stream_once(spool)
            error = "closed by the server"
        except Exception as exc:  # noqa: BLE001 - any stream failure: reconnect
            error = f"{type(exc).__name__}: {exc}"
        spool.write({"r": now_ms(), "c": "close"})
        delay = 1 if time.monotonic() - started > 300 else min(delay * 2, RECONNECT_MAX_S)
        print(f"Stream {error}; reconnecting in {delay} s", file=sys.stderr, flush=True)
        await asyncio.sleep(delay)


async def seal_forever(spool_dir: Path, output_dir: Path) -> None:
    while True:
        for batch in seal_finished(spool_dir, output_dir, now_ms()):
            print(f"Sealed {batch.name}", flush=True)
        await asyncio.sleep(SEAL_CHECK_S)


async def collect(spool_dir: Path, output_dir: Path) -> None:
    """Stream and seal until either fails; a sealing error ends the process (the compose
    restart policy starts it again, and the error is in its log) rather than be retried."""
    spool = Spool(spool_dir)
    try:
        await asyncio.gather(stream_forever(spool), seal_forever(spool_dir, output_dir))
    finally:
        spool.close()


def main() -> int:
    if pipeline_mode(os.environ) != OWN:
        print("PIPELINE_MODE is not 'own': this pipeline does not write the landing zone")
        return 2
    print(f"Collecting {STREAM_URL} into {OUTPUT_DIR} (spool {SPOOL_DIR})", flush=True)
    asyncio.run(collect(SPOOL_DIR, OUTPUT_DIR))
    return 0


if __name__ == "__main__":
    sys.exit(main())
