"""What a dbt call reports in its own artifacts: no connection to the warehouse needed.

dbt writes run_results.json (build, test) or sources.json (source freshness) into
dbt/target of the scheduler container, where the tasks run. Every test result carries its
status, its number of failures and the relation that keeps the failing rows
(store_failures), so an alert can name the checks and where their rows are even when
the warehouse file cannot be opened (another writer holds its lock) or the DQ log hook
itself failed.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

DBT_TARGET = Path("/opt/project/dbt/target")
RUN_RESULTS = "run_results.json"
SOURCES = "sources.json"
PROBLEM_STATUSES = ("error", "fail", "runtime error")
WARN_STATUS = "warn"
MAX_LINES = 12
MAX_MESSAGE_CHARS = 300

Artifact = dict[str, Any]


def read_fresh_artifacts(target: Path, not_before: datetime) -> dict[str, Artifact]:
    """The artifacts generated at or after `not_before`, by file name.

    `not_before` is the start of the task: an artifact left by an earlier dbt call (the
    build before a failed DQ log check, the freshness of the previous run) is not news
    about this one. A missing or unreadable file is no artifact.

    "Newer than the task" means "written by the task" only while one dbt call runs at a
    time in this container: the DAG runs its dbt tasks one after another, one run at a
    time (max_active_runs=1), and dbt by hand goes through the `project` container, which
    has a dbt/target of its own.
    """
    if not_before.utcoffset() is None:
        raise ValueError("not_before must be timezone-aware")
    fresh = {}
    for name in (RUN_RESULTS, SOURCES):
        try:
            artifact = json.loads((target / name).read_text(encoding="utf-8"))
            generated_at = datetime.fromisoformat(artifact["metadata"]["generated_at"])
        except (OSError, ValueError, KeyError, TypeError):
            continue
        if generated_at.utcoffset() is not None and generated_at >= not_before:
            fresh[name] = artifact
    return fresh


def node_name(unique_id: str) -> str:
    """test.pkg.name[.hash] -> name; unit_test.pkg.model.test and source.pkg.src.table
    keep their last two parts."""
    parts = unique_id.split(".")
    if parts[0] in ("unit_test", "source") and len(parts) >= 4:
        return f"{parts[2]}.{parts[3]}"
    return parts[2] if len(parts) >= 3 else unique_id


def first_line(text: str) -> str:
    line = text.strip().splitlines()[0] if text.strip() else ""
    return line if len(line) <= MAX_MESSAGE_CHARS else line[: MAX_MESSAGE_CHARS - 1] + "…"


def result_line(result: dict[str, Any]) -> str:
    """One finding: status, node, what it found and where its rows are."""
    status = str(result.get("status", "?"))
    name = node_name(str(result.get("unique_id", "?")))
    label = f"{status.upper()} {name}"
    if "max_loaded_at_time_ago_in_s" in result and status != "runtime error":
        hours = float(result["max_loaded_at_time_ago_in_s"]) / 3600
        return f"{label}: newest {result.get('max_loaded_at')}, {hours:.1f} h old"
    failures = result.get("failures")
    relation = result.get("relation_name")
    if status in ("fail", "warn") and failures is not None:
        line = f"{label}: {failures} rows"
        return f"{line}\n  select * from {relation}" if relation else line
    message = result.get("message") or result.get("error") or ""
    return f"{label}: {first_line(str(message))}" if message else label


def summarize(artifact: Artifact) -> list[str]:
    """Problems first, then warnings, then how many nodes were skipped."""
    problems, warnings, skipped = [], [], 0
    for result in artifact.get("results", []):
        status = result.get("status")
        if status in PROBLEM_STATUSES:
            problems.append(result_line(result))
        elif status == WARN_STATUS:
            warnings.append(result_line(result))
        elif status == "skipped":
            skipped += 1
    lines = problems + warnings
    if len(lines) > MAX_LINES:
        lines = lines[:MAX_LINES] + [f"… and {len(lines) - MAX_LINES} more"]
    if skipped:
        lines.append(f"skipped: {skipped}")
    return lines


def command_label(name: str, artifact: Artifact) -> str:
    if name == SOURCES:
        return "dbt source freshness"
    return f"dbt {artifact.get('args', {}).get('which', '?')}"


def warning_counts(artifact: Artifact) -> dict[str, tuple[int, str | None]]:
    """Tests that warned: name -> (failures, relation of the failing rows)."""
    counts = {}
    for result in artifact.get("results", []):
        if result.get("status") == WARN_STATUS and result.get("failures") is not None:
            name = node_name(str(result.get("unique_id", "?")))
            counts[name] = (int(result["failures"]), result.get("relation_name"))
    return counts


def warning_changes(
    previous: dict[str, int], current: dict[str, int]
) -> tuple[list[str], list[str]]:
    """Checks that warn more than in the previous run (new ones included), and checks
    that stopped warning. A warning that only stays or shrinks is no news."""
    grown = sorted(name for name, n in current.items() if n > previous.get(name, 0))
    resolved = sorted(name for name in previous if name not in current)
    return grown, resolved
