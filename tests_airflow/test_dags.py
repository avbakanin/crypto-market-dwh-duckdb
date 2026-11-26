"""DAG integrity tests. They need Airflow, so they run with Airflow's interpreter in the image:

    docker run --rm --entrypoint python crypto-market-dwh-duckdb:test -m pytest -q tests_airflow

(the project venv has no Airflow; Airflow's has no project dependencies).

The pipeline DAG is built by PIPELINE_MODE when its file is parsed, so every DagBag here is
parsed with the mode set (and unset, for the error).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.sdk import DAG, CronTriggerTimetable, TriggerRule

from alerting.callbacks import heartbeat, run_failure_alert, task_failure_alert, warning_digest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
PROJECT_BIN = "/opt/project-venv/bin/"
MODES = ("follow", "own")


def parse(mode: str | None) -> DagBag:
    with pytest.MonkeyPatch.context() as env:
        if mode is None:
            env.delenv("PIPELINE_MODE", raising=False)
        else:
            env.setenv("PIPELINE_MODE", mode)
        return DagBag(dag_folder=DAGS_DIR)


@pytest.fixture(scope="module", params=MODES)
def dagbag(request: pytest.FixtureRequest) -> DagBag:
    return parse(request.param)


def test_every_dag_file_imports(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}
    assert dagbag.dag_ids, "no DAGs found"


def test_every_dag_is_paused_upon_creation_and_tagged(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        assert dag.is_paused_upon_creation is True, dag_id
        assert dag.tags, dag_id


def test_every_bash_task_runs_from_the_project_venv(dagbag: DagBag) -> None:
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            command = getattr(task, "bash_command", None)
            if command is not None:
                assert command.startswith(PROJECT_BIN), f"{dag.dag_id}.{task.task_id}"
                assert task.cwd == "/opt/project", f"{dag.dag_id}.{task.task_id}"


def test_smoke_environment_has_no_schedule_and_checks_before_dbt(dagbag: DagBag) -> None:
    dag = dagbag.dags["smoke_environment"]
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.task_dict["dbt_parse"].upstream_task_ids == {"check_environment"}


@pytest.mark.parametrize(
    "mode", [None, "", "Follow", "both"], ids=["unset", "empty", "case", "other"]
)
def test_the_pipeline_without_a_known_mode_is_an_import_error(mode: str | None) -> None:
    dagbag = parse(mode)
    assert "crypto_pipeline" not in dagbag.dag_ids
    assert "smoke_environment" in dagbag.dag_ids  # the other DAGs are not affected
    (error,) = dagbag.import_errors.values()
    assert "PIPELINE_MODE must be one of follow, own" in error


# --- the pipeline ------------------------------------------------------------------------------

EXTRACTS = {
    "extract_klines",
    "extract_funding",
    "extract_open_interest",
    "extract_long_short",
}
LOAD_AND_AFTER = {
    "load_raw",
    "source_freshness",
    "dbt_build",
    "dq_log",
    "export_marts",
}


def extracts_of(pipeline: DAG) -> set[str]:
    return EXTRACTS if "own" in pipeline.tags else set()


@pytest.fixture(scope="module", params=MODES)
def pipeline(request: pytest.FixtureRequest) -> DAG:
    return parse(request.param).dags["crypto_pipeline"]


@pytest.fixture(scope="module")
def follower() -> DAG:
    return parse("follow").dags["crypto_pipeline"]


@pytest.fixture(scope="module")
def owner() -> DAG:
    return parse("own").dags["crypto_pipeline"]


def test_the_pipeline_is_tagged_with_its_mode(follower: DAG, owner: DAG) -> None:
    assert set(follower.tags) == {"pipeline", "follow"}
    assert set(owner.tags) == {"pipeline", "own"}


def test_a_follower_runs_half_an_hour_after_the_original_an_owner_at_its_time(
    follower: DAG, owner: DAG
) -> None:
    assert follower.schedule == "37 */3 * * *"
    assert owner.schedule == "7 */3 * * *"


def test_pipeline_runs_on_a_cron_one_run_at_a_time(pipeline: DAG) -> None:
    assert isinstance(pipeline.timetable, CronTriggerTimetable)
    assert pipeline.catchup is False
    assert pipeline.max_active_runs == 1  # one writer of the DuckDB file


def test_a_follower_has_no_extract_and_loads_after_the_check(follower: DAG) -> None:
    upstream = {task_id: task.upstream_task_ids for task_id, task in follower.task_dict.items()}
    assert upstream == {
        "check_environment": set(),
        "load_raw": {"check_environment"},
        "source_freshness": {"load_raw"},
        "dbt_build": {"source_freshness"},
        "dq_log": {"dbt_build"},
        "export_marts": {"dq_log"},
    }


def test_an_owner_extracts_in_parallel_between_the_check_and_the_load(owner: DAG) -> None:
    upstream = {task_id: task.upstream_task_ids for task_id, task in owner.task_dict.items()}
    assert upstream == {
        "check_environment": set(),
        **{extract: {"check_environment"} for extract in EXTRACTS},
        "load_raw": EXTRACTS,
        "source_freshness": {"load_raw"},
        "dbt_build": {"source_freshness"},
        "dq_log": {"dbt_build"},
        "export_marts": {"dq_log"},
    }


def test_only_nothing_new_skips_the_extracts_and_the_load(pipeline: DAG) -> None:
    skipping = extracts_of(pipeline) | {"load_raw"}
    for task_id, task in pipeline.task_dict.items():
        if task_id in skipping:
            assert task.bash_command.endswith("--nothing-new-exit-code 99"), task_id
            assert list(task.skip_on_exit_code) == [99], task_id
        else:
            assert list(task.skip_on_exit_code) == [], task_id


def test_the_load_runs_unless_an_extract_failed(pipeline: DAG) -> None:
    """All extracts skipped still loads: batches of the original or of a past run may wait."""
    for task_id, task in pipeline.task_dict.items():
        expected = TriggerRule.NONE_FAILED if task_id == "load_raw" else TriggerRule.ALL_SUCCESS
        assert task.trigger_rule == expected, task_id


def test_retries_only_where_a_retry_can_help(pipeline: DAG) -> None:
    retries = {task_id: task.retries for task_id, task in pipeline.task_dict.items()}
    assert retries == {
        "check_environment": 0,
        **dict.fromkeys(extracts_of(pipeline), 2),
        "load_raw": 1,
        "source_freshness": 0,
        "dbt_build": 0,
        "dq_log": 0,
        "export_marts": 1,
    }


def test_pipeline_tasks_have_a_timeout_and_push_no_xcom(pipeline: DAG) -> None:
    for task_id, task in pipeline.task_dict.items():
        assert task.execution_timeout is not None, task_id
        assert task.do_xcom_push is False, task_id


def test_load_and_dbt_commands(pipeline: DAG) -> None:
    """The build uses the default selector; freshness must select its sources itself."""
    commands = {task_id: task.bash_command for task_id, task in pipeline.task_dict.items()}
    load = "python -m ingestion.load_raw --nothing-new-exit-code 99"
    freshness = "dbt source freshness --select source:binance"  # the selector has no source
    assert commands["load_raw"] == f"{PROJECT_BIN}{load}"
    assert commands["source_freshness"] == f"{PROJECT_BIN}{freshness}"
    assert commands["dbt_build"] == f"{PROJECT_BIN}dbt build"
    assert commands["dq_log"] == f"{PROJECT_BIN}dbt test --select tag:dq_log"
    assert set(commands) == {"check_environment"} | extracts_of(pipeline) | LOAD_AND_AFTER


def test_the_export_reads_the_marts_last_and_only_after_every_check_passed(pipeline: DAG) -> None:
    export = pipeline.task_dict["export_marts"]
    assert export.bash_command == f"{PROJECT_BIN}python -m ingestion.export_marts"
    assert export.downstream_task_ids == set()
    assert export.trigger_rule == TriggerRule.ALL_SUCCESS


def _callbacks(value: object) -> list[object]:
    if value is None:
        return []
    return list(value) if isinstance(value, list | tuple) else [value]


def test_every_pipeline_task_alerts_on_its_final_failure_only(pipeline: DAG) -> None:
    for task_id, task in pipeline.task_dict.items():
        assert _callbacks(task.on_failure_callback) == [task_failure_alert], task_id
        assert _callbacks(task.on_retry_callback) == [], task_id  # a retry is not news yet


def test_the_build_sends_the_warning_digest_and_no_other_task_reports_success(
    pipeline: DAG,
) -> None:
    for task_id, task in pipeline.task_dict.items():
        expected = [warning_digest] if task_id == "dbt_build" else []
        assert _callbacks(task.on_success_callback) == expected, task_id


def test_the_run_reports_timeouts_and_pings_the_watchdog_on_success(pipeline: DAG) -> None:
    assert _callbacks(pipeline.on_failure_callback) == [run_failure_alert]
    assert _callbacks(pipeline.on_success_callback) == [heartbeat]
