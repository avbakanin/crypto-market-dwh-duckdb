"""The daily step of the forward test (Airflow task `signals`, after the DQ log check).

    python -m signals.run [--no-telegram] [--nothing-new-exit-code 99]

1. Verify the signal log's chain (any break stops here).
2. Days whose entry (D 01:00 UTC) passed without a decision are logged as missed.
3. Today, if not logged and before its entry: check today's features are complete, load
   (or train once and freeze) the block's model, decide every version, write the day's
   file, send it to Telegram.
Exit 0 when a file was written, the nothing-new code when today was already logged,
1 when the features are not ready or the log is broken (the next run comes after the entry
and logs the day as missed: a late decision would see the entry price).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from research import data
from signals import daily, journal, models, notify, registry

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SIGNALS_DIR = PROJECT_ROOT / "data" / "signals"
WAREHOUSE = PROJECT_ROOT / "warehouse" / "crypto_dwh.duckdb"
UNIVERSE_SIZE = 100  # dbt var um_universe_size


def git_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, capture_output=True, text=True,
        check=False,
    ).stdout.strip() or "unknown"  # fmt: skip


def connect(warehouse: Path):
    import duckdb

    con = duckdb.connect(str(warehouse), read_only=True)
    con.execute("set TimeZone = 'UTC'")
    return con


def read_day(warehouse: Path, day: date) -> tuple[pd.DataFrame, dict[str, Any], list[dict]]:
    """D's features, the context (DQ of the last build) and the forward results so far."""
    with connect(warehouse) as con:
        features = con.execute(
            "select * from staging.int_um__features_daily where decision_date = ?", [day]
        ).df()
        build = con.execute(
            """
            with last as (
                select invocation_id, max(run_started_at) as started
                from dq.dbt_node_results where dbt_command = 'build'
                group by 1 order by started desc limit 1
            )
            select any_value(last.started),
                   count(*) filter (where status in ('error', 'fail')),
                   count(*) filter (where status = 'warn')
            from dq.dbt_node_results inner join last using (invocation_id)
            """
        ).fetchone()
        try:
            performance = (
                con.execute("select * from marts.mart_signal_performance order by version")
                .df()
                .to_dict("records")
            )
        except Exception:  # the mart is built from the log: absent before the first day
            performance = []
    started, errors, warns = build or (None, None, None)
    context = {
        "feature_date": str(day - timedelta(days=1)),
        "members": len(features),
        "dq": {
            "last_build": f"{started:%Y-%m-%d %H:%M} UTC" if started else None,
            "errors": errors,
            "warn_findings": warns,
        },
        "git_commit": git_commit(),
    }
    return features, context, performance


def labelled_panel(warehouse: Path) -> pd.DataFrame:
    features, labels = data.read_warehouse(warehouse)
    return data.build_panel(features, labels, include_holdout=True)


def decide_today(
    day: date,
    features: pd.DataFrame,
    history: list[dict[str, Any]],
    models_dir: Path,
    warehouse: Path,
) -> list[dict[str, Any]]:
    daily.check_day(features, day, UNIVERSE_SIZE)
    block = registry.block_start(day)
    frozen: dict[str, models.FrozenModel] = {}
    decisions = []
    for version in registry.VERSIONS:
        if version.model_key not in frozen:
            frozen[version.model_key] = models.ensure(
                models_dir, version, block, lambda: labelled_panel(warehouse)
            )
        decisions.append(daily.decide(version, frozen[version.model_key], features, history, day))
    return decisions


def utc_now() -> datetime:
    return datetime.now(UTC)


def main(argv: Sequence[str] | None = None, clock: Callable[[], datetime] = utc_now) -> int:
    parser = argparse.ArgumentParser(description="Log today's forward-test decisions.")
    parser.add_argument("--signals-dir", type=Path, default=SIGNALS_DIR)
    parser.add_argument("--warehouse", type=Path, default=WAREHOUSE)
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--nothing-new-exit-code", type=int, default=0)
    args = parser.parse_args(argv)
    now = clock()
    log_dir, models_dir = args.signals_dir / "log", args.signals_dir / "models"
    today = now.astimezone(UTC).date()

    try:
        last, prev_hash = journal.verify(log_dir)
    except journal.JournalError as error:
        print(f"ERROR: {error}")
        return 1
    if today < registry.FORWARD_START:
        print(f"the forward test starts on {registry.FORWARD_START}")
        return args.nothing_new_exit_code

    day = registry.FORWARD_START if last is None else last + timedelta(days=1)
    late: list[date] = []
    while day <= today and now >= journal.entry_time(day):
        prev_hash = journal.write_day(
            log_dir, day, journal.missed("late"), prev_hash, now, {"git_commit": git_commit()}
        )
        late.append(day)
        day += timedelta(days=1)
    if late:
        print(f"logged as missed: {late[0]} .. {late[-1]}")
        if not args.no_telegram:
            notify.send(notify.missed_message(late))
    if day > today:
        if not late:
            print(f"{today} is logged already")
        return 0 if late else args.nothing_new_exit_code

    features, context, performance = read_day(args.warehouse, day)
    history = journal.read_all(log_dir)
    try:
        decisions = decide_today(day, features, history, models_dir, args.warehouse)
    except (daily.DataNotReady, models.ModelError) as error:
        print(f"ERROR: {error}")
        return 1
    # The decision took time: it still has to be logged before the entry.
    now = clock()
    if now >= journal.entry_time(day):
        print(f"ERROR: {day} decided after its entry; the next run logs it as missed")
        return 1
    sha = journal.write_day(log_dir, day, decisions, prev_hash, now, context)
    print(f"logged {day}: " + "; ".join(notify.describe(d) for d in decisions))
    if not args.no_telegram:
        notify.send(notify.day_message(day, decisions, sha, context, performance))
    return 0


if __name__ == "__main__":
    sys.exit(main())
