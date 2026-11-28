"""The hypothesis ledger: every hypothesis ever tested on the history, across runs.

Holm in research.run corrects within one run; an idea tried in a later run, or by the event
study, is one more look at the same data. The ledger counts them all, so a new result is
judged against the number of hypotheses tried so far (Bonferroni over the ledger).
Append-only JSON lines in data/research/hypothesis_log.jsonl (not in git, like the runs).
Placebo and holdout runs test no new hypothesis and are not recorded.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "data" / "research"
LEDGER = RESEARCH_DIR / "hypothesis_log.jsonl"


def read(path: Path = LEDGER) -> list[dict]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def tested(path: Path = LEDGER) -> set[str]:
    """Names of every hypothesis recorded so far."""
    return {entry["name"] for entry in read(path)}


def record(names: list[str], kind: str, run_id: str, path: Path = LEDGER) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    at = datetime.now(UTC).isoformat(timespec="seconds")
    with path.open("a", encoding="utf-8", newline="\n") as log:
        for name in names:
            log.write(json.dumps({"name": name, "kind": kind, "run_id": run_id, "at": at}) + "\n")


def backfill(research_dir: Path = RESEARCH_DIR, path: Path = LEDGER) -> int:
    """Once, when there is no ledger yet: record the versions of the walk-forward runs made
    before it existed. Returns the number of entries written."""
    if path.exists():
        return 0
    written = 0
    for manifest_path in sorted(research_dir.glob("*_walk_forward/manifest.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        new = [v for v in manifest["versions"] if v not in tested(path)]
        if new:
            record(new, "walk_forward", manifest["run_id"], path)
            written += len(new)
    return written


def adjusted_p(p: float, attempts: int) -> float:
    """Bonferroni over every hypothesis tried: conservative, valid under any dependence."""
    return min(1.0, p * max(attempts, 1))
