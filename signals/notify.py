"""The Telegram message of a decision day (a paper test until the criteria are met)."""

from __future__ import annotations

import importlib
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from signals import journal, registry

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# Research numbers of the source version (docs/signal_research.md).
RESEARCH_NOTE = (
    "research: walk-forward 2022-25H1 +3.4 %/trade (Holm p 0.012); "
    "holdout 2025-07..2026-11 +0.65 %, p 0.42 - not confirmed"
)


def local(moment: datetime) -> str:
    """UTC and the user's UTC+1."""
    return f"{moment:%Y-%m-%d %H:%M} UTC ({moment + timedelta(hours=1):%H:%M} UTC+1)"


def describe(decision: dict[str, Any]) -> str:
    name = decision["version"]
    if decision["status"] == journal.MISSED:
        return f"{name}: missed ({decision['reason']})"
    if decision["status"] == journal.NO_TRADE:
        return f"{name}: no trade ({decision['reason']})"
    text = f"{name}: {decision['side'].upper()} {decision['symbol']}"
    if decision.get("hedge_symbol") and not decision.get("hedge_ratio"):
        text += f" (no {decision['hedge_symbol']} hedge: beta <= 0)"
    elif decision.get("hedge_symbol"):
        text += (
            f" + {decision['hedge_side'].upper()} {decision['hedge_symbol']}"
            f" x{decision['hedge_ratio']:.2f} of its notional"
        )
    text += f", hold {decision['horizon_days']} d (exit {decision['exit_time'][:16]} UTC)"
    if decision.get("stop_loss"):
        text += f", close both if {decision['symbol']} moves {decision['stop_loss']:.0%} against"
    return text


def count(value: Any) -> int:
    """A count from the mart: NULL (NaN or None in the frame) is 0."""
    return 0 if value is None or value != value else int(value)


def performance_line(row: dict[str, Any]) -> str:
    closed, still_open = count(row.get("closed_trades")), count(row.get("open_trades"))
    if not closed:
        return f"{row['version']}: no closed trade yet ({still_open} open)"
    return (
        f"{row['version']}: {closed} closed, mean net {row['mean_net']:+.2%}, "
        f"hit {row['hit_rate']:.0%}, {still_open} open"
    )


def day_message(
    day: date,
    decisions: list[dict[str, Any]],
    sha256: str,
    context: dict[str, Any],
    performance: list[dict[str, Any]],
) -> str:
    lines = [
        f"📝 PAPER TEST - signals of {day} (entry {local(journal.entry_time(day))})",
        *[describe(d) for d in sorted(decisions, key=lambda d: d["version"])],
        RESEARCH_NOTE,
    ]
    lines += [f"forward: {performance_line(row)}" for row in performance]
    dq = context.get("dq", {})
    lines.append(
        f"data: features of {context.get('feature_date')} for {context.get('members')} members; "
        f"dbt build {dq.get('last_build', '?')}: {dq.get('errors', '?')} errors, "
        f"{dq.get('warn_findings', '?')} warn checks with rows"
    )
    lines.append(f"log {day}.json sha256 {sha256[:16]}")
    lines.append(
        f"Not a confirmed signal: paper until >= {registry.CRITERIA_MIN_DAYS} days, "
        f">= {registry.CRITERIA_MIN_CLOSED} closed trades and a 95 % interval above 0."
    )
    return "\n".join(lines)


def missed_message(days: list[date]) -> str:
    span = f"{days[0]}" if len(days) == 1 else f"{days[0]} .. {days[-1]}"
    return (
        f"⏭ PAPER TEST - signals of {span} missed: no run before the entry. "
        "Logged as missed (no trade)."
    )


def send(text: str) -> None:
    """Through the DAG's alert channel (dags/alerting/channels.py, standard library only)."""
    dags = str(PROJECT_ROOT / "dags")
    if dags not in sys.path:
        sys.path.append(dags)
    channels = importlib.import_module("alerting.channels")
    channels.send_telegram(text)
