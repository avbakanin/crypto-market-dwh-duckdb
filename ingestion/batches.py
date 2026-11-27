"""Landing-zone contract between extract and load: one immutable directory per extract run.

    data/raw/klines_1h/<batch_id>/<SYMBOL>.json   klines exactly as received
    data/raw/klines_1h/<batch_id>/_manifest.json  written last: marks the batch complete

A batch is written once and never changed. The loader skips a batch whose id is already
in RAW, so a file changed in place after its load would never reach the warehouse, and the
manifest would no longer describe what RAW holds.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NotRequired, TypedDict, cast

BATCH_ID_FORMAT = "%Y%m%dT%H%M%SZ"  # UTC as-of moment: sorts chronologically, safe in paths
BATCH_ID_PATTERN = re.compile(r"\d{8}T\d{6}Z")
MANIFEST_NAME = "_manifest.json"


class Manifest(TypedDict):
    batch_id: str
    as_of_utc: str
    interval: str
    # symbol -> stats of its file: DQ counts (rows, duplicates, gaps, expected) plus the
    # newest record time in the file: last_ms (klines: last_open_ms; absent before Week 2)
    symbols: dict[str, dict[str, int]]
    # symbol -> {source file name: sha256}: archive extracts only (extract_archive.py)
    sources: NotRequired[dict[str, dict[str, str]]]


def batch_id_for(as_of_ms: int) -> str:
    """Batch id for an extract run: its as-of moment in UTC, truncated to the second."""
    return datetime.fromtimestamp(as_of_ms // 1000, tz=UTC).strftime(BATCH_ID_FORMAT)


def is_batch_id(value: str) -> bool:
    return BATCH_ID_PATTERN.fullmatch(value) is not None


def write_manifest(batch_dir: Path, manifest: Manifest) -> None:
    (batch_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def read_manifest(batch_dir: Path) -> Manifest:
    """Read a batch's manifest; refuse one that describes a different batch."""
    manifest = cast(Manifest, json.loads((batch_dir / MANIFEST_NAME).read_text(encoding="utf-8")))
    if manifest["batch_id"] != batch_dir.name:
        raise ValueError(f"Manifest in {batch_dir.name} describes batch {manifest['batch_id']!r}")
    return manifest


class BatchWriter:
    """Writes one batch of any dataset, keeping the landing-zone invariants in one place:

    - an existing batch directory is refused when the writer is created, before any request;
    - the directory is created by the first file: a run with nothing new leaves no trace;
    - the manifest is written last, by finish(): a batch without one is incomplete;
    - finish() with no file writes nothing and returns None (an empty batch adds no rows to
      RAW, so the loader would never see it as loaded).
    """

    def __init__(self, output_dir: Path, as_of_ms: int, interval: str) -> None:
        self.batch_dir = output_dir / batch_id_for(as_of_ms)
        if self.batch_dir.exists():  # a batch is written once and never overwritten
            raise FileExistsError(f"Batch {self.batch_dir} already exists")
        self.as_of_utc = datetime.fromtimestamp(as_of_ms / 1000, tz=UTC).isoformat()
        self.interval = interval
        self.symbols: dict[str, dict[str, int]] = {}
        self.sources: dict[str, dict[str, str]] = {}

    def add(
        self,
        symbol: str,
        records: list[Any],
        stats: dict[str, int],
        sources: dict[str, str] | None = None,
    ) -> None:
        """Write one symbol's file exactly as received; its stats (and the source files it
        came from, for an archive extract) go into the manifest."""
        if symbol in self.symbols:
            raise ValueError(f"{symbol} is already in batch {self.batch_dir.name}")
        if not self.symbols:
            self.batch_dir.mkdir(parents=True)  # no exist_ok: guards a concurrent run too
        (self.batch_dir / f"{symbol}.json").write_text(json.dumps(records), encoding="utf-8")
        self.symbols[symbol] = stats
        if sources is not None:
            self.sources[symbol] = sources

    def finish(self) -> Path | None:
        """Write the manifest, marking the batch complete; None when nothing was added."""
        if not self.symbols:
            return None
        manifest: Manifest = {
            "batch_id": self.batch_dir.name,
            "as_of_utc": self.as_of_utc,
            "interval": self.interval,
            "symbols": self.symbols,
        }
        if self.sources:
            manifest["sources"] = self.sources
        write_manifest(self.batch_dir, manifest)
        return self.batch_dir


def complete_batches(root: Path) -> list[Path]:
    """Batch directories that have a manifest, oldest first; batches without one are incomplete."""
    if not root.is_dir():
        return []
    complete = [d for d in root.iterdir() if is_batch_id(d.name) and (d / MANIFEST_NAME).is_file()]
    return sorted(complete, key=lambda d: d.name)


def latest_complete_batch(root: Path) -> Path:
    """Newest batch directory that has a manifest."""
    if not root.is_dir():
        raise FileNotFoundError(f"No batch directory {root}")
    complete = complete_batches(root)
    if not complete:
        raise FileNotFoundError(f"No complete batch in {root}")
    return complete[-1]


LAST_TIME_KEYS = ("last_ms", "last_open_ms")  # any dataset; klines manifests of Week 2


def last_time_ms(batch_dir: Path, symbol: str, stats: dict[str, int]) -> int | None:
    """Newest record time in a batch file: from the manifest, else (klines manifests written
    before Week 2) the newest open_time in the file."""
    for key in LAST_TIME_KEYS:
        if key in stats:
            return stats[key]
    # The batch is immutable, so an old manifest is never rewritten: read its file instead.
    klines = json.loads((batch_dir / f"{symbol}.json").read_text(encoding="utf-8"))
    return max((k[0] for k in klines), default=None)


def high_water_marks(root: Path) -> dict[str, int]:
    """Extract watermark: newest record time already extracted, per symbol, over complete batches.

    It answers "what has been extracted", not "what has been loaded": the loader closes that
    gap by loading every complete batch that is not in RAW yet. No batches -> no marks.
    """
    marks: dict[str, int] = {}
    for batch_dir in complete_batches(root):
        for symbol, stats in read_manifest(batch_dir)["symbols"].items():
            last = last_time_ms(batch_dir, symbol, stats)
            if last is not None:
                marks[symbol] = max(marks.get(symbol, last), last)
    return marks
