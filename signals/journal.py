"""The signal log: one file per decision day, written once before the day's entry, chained.

data/signals/log/<YYYY-MM-DD>.json holds every registered version's decision of that day:
a trade, no trade (and why), or missed (the run came after the entry, or not at all). Each
file carries the SHA-256 of the previous day's file, so a changed or removed day breaks the
chain; days run without gaps from registry.FORWARD_START. The Telegram message of a day
quotes its file's hash: an outside, timestamped copy of what was logged.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from signals import registry
from signals.models import create_once

GENESIS = "0" * 64
NAME = re.compile(r"^(\d{4}-\d{2}-\d{2})\.json$")
# Statuses of a decision. 'missed' days still count as logged days, with no trade.
TRADE, NO_TRADE, MISSED = "trade", "no_trade", "missed"
ENTRY_DELAY = timedelta(hours=1)  # dbt var entry_delay_hours: the labels' entry candle


class JournalError(RuntimeError):
    """The log is not what it must be: nothing is decided until it is looked at."""


def entry_time(day: date) -> datetime:
    return datetime.combine(day, time(), tzinfo=UTC) + ENTRY_DELAY


def path_for(log_dir: Path, day: date) -> Path:
    return log_dir / f"{day.isoformat()}.json"


def encode(document: dict[str, Any]) -> bytes:
    return (json.dumps(document, indent=1, sort_keys=True) + "\n").encode("ascii")


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def logged_days(log_dir: Path) -> list[date]:
    if not log_dir.exists():
        return []
    days = []
    for path in log_dir.iterdir():
        match = NAME.match(path.name)
        if match:
            days.append(date.fromisoformat(match.group(1)))
        elif not path.name.startswith("."):
            raise JournalError(f"unexpected file in the signal log: {path.name}")
    return sorted(days)


def verify(log_dir: Path) -> tuple[date | None, str]:
    """Check the whole chain; the last logged day and its file's hash (GENESIS if none)."""
    previous_hash, previous_day = GENESIS, None
    names = {v.name for v in registry.VERSIONS}
    for day in logged_days(log_dir):
        expected_day = (
            registry.FORWARD_START if previous_day is None else previous_day + timedelta(1)
        )
        if day != expected_day:
            raise JournalError(f"signal log jumps to {day}, expected {expected_day}")
        body = path_for(log_dir, day).read_bytes()
        document = json.loads(body)
        if document.get("decision_date") != day.isoformat():
            raise JournalError(f"{day}.json: decision_date {document.get('decision_date')}")
        if document.get("prev_sha256") != previous_hash:
            raise JournalError(f"{day}.json: chain broken (previous day's file changed)")
        decisions = document.get("decisions", [])
        if sorted(d["version"] for d in decisions) != sorted(names):
            raise JournalError(f"{day}.json: versions differ from the registry")
        logged_at = datetime.fromisoformat(document["logged_at"])
        if any(d["status"] != MISSED for d in decisions) and logged_at >= entry_time(day):
            raise JournalError(f"{day}.json: a decision logged after the entry")
        previous_hash, previous_day = sha256_bytes(body), day
    return previous_day, previous_hash


def read_all(log_dir: Path) -> list[dict[str, Any]]:
    """Every logged decision with its day, oldest first (after verify)."""
    rows = []
    for day in logged_days(log_dir):
        document = json.loads(path_for(log_dir, day).read_bytes())
        for decision in document["decisions"]:
            rows.append({"decision_date": day, **decision})
    return rows


def write_day(
    log_dir: Path,
    day: date,
    decisions: list[dict[str, Any]],
    prev_sha256: str,
    now: datetime,
    context: dict[str, Any],
) -> str:
    """Write the day's file (refused if it exists); its hash."""
    document = {
        "decision_date": day.isoformat(),
        "entry_time": entry_time(day).isoformat(),
        "logged_at": now.astimezone(UTC).isoformat(),
        "prev_sha256": prev_sha256,
        "registered_on": registry.REGISTERED_ON.isoformat(),
        "context": context,
        "decisions": sorted(decisions, key=lambda d: d["version"]),
    }
    body = encode(document)
    create_once(path_for(log_dir, day), body)
    return sha256_bytes(body)


def missed(reason: str) -> list[dict[str, Any]]:
    return [
        {"version": v.name, "status": MISSED, "reason": reason, "horizon_days": v.horizon}
        for v in registry.VERSIONS
    ]
