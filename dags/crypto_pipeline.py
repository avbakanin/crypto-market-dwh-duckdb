"""The pipeline: Binance -> landing zone -> DuckDB RAW -> dbt -> files for the dashboard.

Its shape depends on PIPELINE_MODE (who owns the landing zone, see ingestion/settings.py):

follow  check_environment -> load_raw -> source_freshness -> dbt_build -> dq_log -> export_marts
        dq_log -> signals (next to export_marts, in both modes)
        The Snowflake original extracts into the landing zone (at :07) and this stack mounts
        it read-only: no extract tasks, and the run comes 30 minutes later, at :37.
own     check_environment -> extract x6 (in parallel) -> load_raw -> ... (as above), at :07.

* The extracts work from their high-water marks, not from the run's logical date: a run
  after any downtime catches up by itself (open interest within its 30-day window), so
  there is no catchup and no backfill. The same holds for the loader: it loads every
  complete batch that is not in RAW yet.
* "Nothing new" exits 99, which BashOperator reports as skipped: an extract that wrote no
  batch, and the loader when no dataset had a batch to load. load_raw runs when no extract
  failed (all skipped is fine: the original may have written batches, or a past run's
  batches may wait); when it skips, everything downstream skips and nothing is rebuilt.
* Failures of dbt are findings about the data, not transient errors: no retries there.
  The DQ log is reconciled in a second dbt call, after the hook of the build has written it.
* The marts are exported to data/marts/ (Parquet, from the warehouse opened read-only) only
  after the build and the reconciliation passed: the dashboard reads the last checked state.
* The forward test (signals/, docs/forward_test.md): after the DQ log check, next to the
  export (both only read the warehouse), the first run after 00:00 UTC logs the day's
  decisions to data/signals/log/ and sends them to Telegram as a paper test; later runs
  of the day find it logged (skipped). A day whose entry (01:00 UTC) passes without a
  decision is logged as missed by the next run.
* One writer of the DuckDB file: the tasks run one after another and one run at a time.
* Alerts (dags/alerting, messages start with "[duckdb]"): a Telegram message when a task
  fails for good (with the failed dbt checks and where their rows are, or the last output
  of the command) or the run times out, and when a warn-level DQ check finds more rows than
  in the previous build. Every successful run pings an outside watchdog, which alerts when
  the pings stop.

The mode is read when the file is parsed: the DAG processor and the scheduler both get it
from docker-compose.yaml. An unset or unknown mode is an import error, shown in the UI.

Trigger it by hand with "Drain" (the run goes, the DAG stays paused) before unpausing it.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule

from alerting.callbacks import heartbeat, run_failure_alert, task_failure_alert, warning_digest

PROJECT_HOME = "/opt/project"
# The project's own venv, not Airflow's: see the Dockerfile.
PROJECT_BIN = "/opt/project-venv/bin"
# The values of ingestion.settings.PIPELINE_MODES (the DAG cannot import the project: it
# runs in Airflow's interpreter).
FOLLOW = "follow"
OWN = "own"
# At :07 the last hour's candle is closed and the open interest point and funding
# settlement of the hour are published (lags of about a minute; 5 minutes assumed). A
# follower comes 30 minutes after the original's run, which takes 3-5 minutes.
# Every 3 hours: completeness does not depend on the cadence.
SCHEDULES = {FOLLOW: "37 */3 * * *", OWN: "7 */3 * * *"}
SKIPPED = 99  # BashOperator's default skip_on_exit_code
# The whitelist's four, then the USD-M universe: data.binance.vision history (a day is
# published the morning after) and the API tail of the universe's members.
EXTRACTS = ("klines", "funding", "open_interest", "long_short", "archive", "um_api")

MODE = os.environ.get("PIPELINE_MODE", "")
if MODE not in SCHEDULES:
    raise ValueError(f"PIPELINE_MODE must be one of {', '.join(SCHEDULES)}")


def project_task(task_id: str, command: str, **kwargs: Any) -> BashOperator:
    """A command of the project venv, run from the project root."""
    kwargs.setdefault("skip_on_exit_code", None)  # only "nothing new" may skip
    return BashOperator(
        task_id=task_id,
        bash_command=f"{PROJECT_BIN}/{command}",
        cwd=PROJECT_HOME,
        **kwargs,
    )


with DAG(
    dag_id="crypto_pipeline",
    description=f"Load, transform and check the crypto market data (mode: {MODE})",
    schedule=SCHEDULES[MODE],
    catchup=False,
    max_active_runs=1,  # one writer of the DuckDB file; idempotent steps still must not race
    dagrun_timeout=timedelta(hours=2),
    is_paused_upon_creation=True,
    tags=["pipeline", MODE],
    doc_md=__doc__,
    on_failure_callback=run_failure_alert,  # timed out or deadlocked, not task failures
    on_success_callback=heartbeat,  # every successful run, all-skipped ones included
    default_args={
        "retries": 0,
        "do_xcom_push": False,  # no task needs its output in the metadata database
        "execution_timeout": timedelta(minutes=20),
        # Only a final failure: a retry is not news yet, upstream_failed has no callback.
        "on_failure_callback": task_failure_alert,
    },
):
    check_environment = project_task(
        "check_environment",
        "python -m ingestion.check_environment",
        execution_timeout=timedelta(minutes=2),
    )
    # Retries on top of the client's own (5 attempts, seconds apart): outages of minutes.
    extracts = [
        project_task(
            f"extract_{name}",
            f"python -m ingestion.extract_{name} --nothing-new-exit-code {SKIPPED}",
            skip_on_exit_code=SKIPPED,
            retries=2,
            retry_delay=timedelta(minutes=5),
            retry_exponential_backoff=True,
        )
        for name in (EXTRACTS if MODE == OWN else ())
    ]
    # Idempotent (a batch loads in one transaction, its own guard): one retry, e.g. for the
    # file's lock held by a command started by hand.
    load_raw = project_task(
        "load_raw",
        f"python -m ingestion.load_raw --nothing-new-exit-code {SKIPPED}",
        skip_on_exit_code=SKIPPED,
        trigger_rule=TriggerRule.NONE_FAILED,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    # The default selector (dbt/selectors.yml) matches no source: without --select,
    # "dbt source freshness" checks nothing and still exits 0.
    source_freshness = project_task(
        "source_freshness", "dbt source freshness --select source:binance"
    )
    dbt_build = project_task("dbt_build", "dbt build", on_success_callback=warning_digest)
    dq_log = project_task("dq_log", "dbt test --select tag:dq_log")
    # Every try writes a new export directory, and an incomplete one is never read and is
    # pruned later: one retry is safe.
    export_marts = project_task(
        "export_marts",
        "python -m ingestion.export_marts",
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
    )

    # Nothing new (today is logged already) skips. Not retried: a retry after 01:00 UTC
    # would only log the day as missed, which the next run does anyway.
    signals = project_task(
        "signals",
        f"python -m signals.run --nothing-new-exit-code {SKIPPED}",
        skip_on_exit_code=SKIPPED,
        execution_timeout=timedelta(minutes=15),
    )

    if extracts:
        check_environment >> extracts >> load_raw
    else:
        check_environment >> load_raw
    load_raw >> source_freshness >> dbt_build >> dq_log >> [export_marts, signals]
