"""Tests of the alerting package (dags/alerting): artifacts, channels, notifiers.

The shapes of run_results.json and sources.json follow live files of dbt 1.12.7 in the
scheduler container; sending goes to a fake urlopen, never to Telegram.
"""

from __future__ import annotations

import io
import json
import urllib.error
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from alerting import callbacks, channels, dbt_artifacts

START = datetime(2026, 11, 22, 15, 7, tzinfo=UTC)
TOKEN = "123456:secret-token"
ENV = {"TELEGRAM_BOT_TOKEN": TOKEN, "TELEGRAM_CHAT_ID": "42"}


def make_result(status: str, name: str, failures: int | None = 0, relation: Any = None, **extra):
    return {
        "status": status,
        "unique_id": f"test.crypto_market_dwh.{name}.0123456789",
        "failures": failures,
        "relation_name": relation,
        "message": None,
        **extra,
    }


def run_results(*results: dict, which: str = "build", at: datetime = START) -> dict:
    stamp = (at + timedelta(seconds=50)).isoformat().replace("+00:00", "Z")
    return {"metadata": {"generated_at": stamp}, "args": {"which": which}, "results": list(results)}


def write(target: Path, name: str, artifact: dict) -> None:
    target.mkdir(parents=True, exist_ok=True)
    (target / name).write_text(json.dumps(artifact), encoding="utf-8")


FAILED_OHLC = make_result(
    "fail",
    "dbt_utils_expression_is_true_stg_binance__klines_1h_ohlc",
    1,
    "CRYPTO_DWH.DQ.dbt_utils_expression_is_true_stg_ohlc",
)
VOLUME_WARN = make_result("warn", "log_volume_zscore", 25, "CRYPTO_DWH.DQ.dq_volume_anomaly")


# --- dbt_artifacts --------------------------------------------------------------------


def test_only_artifacts_generated_since_the_task_started_are_read(tmp_path: Path) -> None:
    write(tmp_path, "run_results.json", run_results(FAILED_OHLC))
    write(tmp_path, "sources.json", run_results(at=START - timedelta(hours=3)))
    assert set(dbt_artifacts.read_fresh_artifacts(tmp_path, START)) == {"run_results.json"}
    assert dbt_artifacts.read_fresh_artifacts(tmp_path, START + timedelta(minutes=5)) == {}


def test_missing_or_broken_artifacts_are_no_artifacts(tmp_path: Path) -> None:
    assert dbt_artifacts.read_fresh_artifacts(tmp_path, START) == {}
    (tmp_path / "run_results.json").write_text("{not json", encoding="utf-8")
    (tmp_path / "sources.json").write_text('{"results": []}', encoding="utf-8")
    assert dbt_artifacts.read_fresh_artifacts(tmp_path, START) == {}


def test_naive_start_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        dbt_artifacts.read_fresh_artifacts(tmp_path, START.replace(tzinfo=None))


@pytest.mark.parametrize(
    ("unique_id", "name"),
    [
        (
            "test.crypto_market_dwh.not_null_dim_symbol_symbol.9f1e2d3c4b",
            "not_null_dim_symbol_symbol",
        ),
        (
            "test.crypto_market_dwh.assert_pump_window_has_every_offset",
            "assert_pump_window_has_every_offset",
        ),
        ("unit_test.crypto_market_dwh.dim_symbol.symbols_join", "dim_symbol.symbols_join"),
        (
            "source.crypto_market_dwh.binance.binance_spot_klines_1h",
            "binance.binance_spot_klines_1h",
        ),
        (
            "operation.crypto_market_dwh.crypto_market_dwh-on-run-end-0",
            "crypto_market_dwh-on-run-end-0",
        ),
    ],
)
def test_node_names(unique_id: str, name: str) -> None:
    assert dbt_artifacts.node_name(unique_id) == name


def test_summary_puts_problems_first_with_their_rows_then_warnings_then_skipped() -> None:
    hook = {
        "status": "error",
        "unique_id": "operation.crypto_market_dwh.crypto_market_dwh-on-run-end-0",
        "failures": None,
        "relation_name": None,
        "message": "Database Error\n  002003: Object does not exist",
    }
    passed = make_result("pass", "unique_dim_symbol_symbol")
    skipped = {"status": "skipped", "unique_id": "model.crypto_market_dwh.fct_pump_events"}
    lines = dbt_artifacts.summarize(run_results(VOLUME_WARN, passed, FAILED_OHLC, skipped, hook))
    assert lines == [
        "FAIL dbt_utils_expression_is_true_stg_binance__klines_1h_ohlc: 1 rows\n"
        "  select * from CRYPTO_DWH.DQ.dbt_utils_expression_is_true_stg_ohlc",
        "ERROR crypto_market_dwh-on-run-end-0: Database Error",
        "WARN log_volume_zscore: 25 rows\n  select * from CRYPTO_DWH.DQ.dq_volume_anomaly",
        "skipped: 1",
    ]


def test_freshness_results_name_the_newest_data_and_its_age() -> None:
    stale = {
        "status": "error",
        "unique_id": "source.crypto_market_dwh.binance.binance_spot_klines_1h",
        "max_loaded_at": "2026-11-21T09:00:00+00:00",
        "max_loaded_at_time_ago_in_s": 30.5 * 3600,
    }
    broken = {
        "status": "runtime error",
        "unique_id": "source.crypto_market_dwh.binance.binance_futures_funding_rate",
        "max_loaded_at_time_ago_in_s": 0,
        "error": "Database Error\n  permission denied",
    }
    assert dbt_artifacts.summarize({"results": [stale, broken]}) == [
        "ERROR binance.binance_spot_klines_1h: newest 2026-11-21T09:00:00+00:00, 30.5 h old",
        "RUNTIME ERROR binance.binance_futures_funding_rate: Database Error",
    ]


def test_long_summaries_are_cut_with_a_count_of_the_rest() -> None:
    failed = [make_result("fail", f"t{i}", 1) for i in range(dbt_artifacts.MAX_LINES + 3)]
    lines = dbt_artifacts.summarize(run_results(*failed))
    assert len(lines) == dbt_artifacts.MAX_LINES + 1
    assert lines[-1] == "… and 3 more"


def test_warning_changes_report_growth_and_resolution_not_steady_or_shrinking() -> None:
    previous = {"steady": 4, "grows": 24, "shrinks": 10, "gone": 2}
    current = {"steady": 4, "grows": 25, "shrinks": 3, "new": 1}
    assert dbt_artifacts.warning_changes(previous, current) == (["grows", "new"], ["gone"])


# --- channels -------------------------------------------------------------------------


class FakeOpener:
    def __init__(self, reply: Any = None, error: BaseException | None = None) -> None:
        self.reply = {"ok": True} if reply is None else reply
        self.error = error
        self.requests: list[Any] = []

    def __call__(self, request: Any, timeout: float) -> io.BytesIO:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return io.BytesIO(json.dumps(self.reply).encode())


def test_telegram_message_is_plain_text_to_the_configured_chat() -> None:
    opener = FakeOpener()
    channels.send_telegram("hello_world", ENV, opener)
    (request,) = opener.requests
    assert request.full_url == f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    body = json.loads(request.data)
    text = "[duckdb] hello_world"  # the original's bot is shared: say which pipeline it is
    assert body == {"chat_id": "42", "text": text, "disable_web_page_preview": True}


def test_api_url_can_point_elsewhere_and_long_text_is_cut() -> None:
    opener = FakeOpener()
    channels.send_telegram("x" * 5000, {**ENV, "TELEGRAM_API_URL": "http://fake:8000"}, opener)
    request = opener.requests[0]
    assert request.full_url.startswith("http://fake:8000/bot")
    text = json.loads(request.data)["text"]
    assert len(text) == channels.MAX_TEXT and text.endswith("…")
    assert text.startswith("[duckdb] x")  # cut at the end: the prefix survives


@pytest.mark.parametrize("missing", ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"])
def test_missing_variable_is_named_before_any_request(missing: str) -> None:
    opener = FakeOpener()
    with pytest.raises(channels.AlertDeliveryError, match=missing):
        channels.send_telegram("x", {**ENV, missing: ""}, opener)
    assert opener.requests == []


def test_refusal_names_the_reason_but_never_the_token() -> None:
    body = io.BytesIO(
        json.dumps({"ok": False, "description": "Bad Request: chat not found"}).encode()
    )
    refusal = urllib.error.HTTPError(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage", 400, "Bad Request", {}, body
    )  # type: ignore[arg-type]
    with pytest.raises(channels.AlertDeliveryError) as raised:
        channels.send_telegram("x", ENV, FakeOpener(error=refusal))
    assert "chat not found" in str(raised.value)
    assert TOKEN not in str(raised.value)
    assert raised.value.__cause__ is None and raised.value.__suppress_context__


def test_unreachable_api_never_shows_the_token() -> None:
    error = urllib.error.URLError(f"cannot reach /bot{TOKEN}/sendMessage")
    with pytest.raises(channels.AlertDeliveryError, match="unreachable") as raised:
        channels.send_telegram("x", ENV, FakeOpener(error=error))
    assert TOKEN not in str(raised.value)


def test_reply_without_ok_is_a_failure() -> None:
    with pytest.raises(channels.AlertDeliveryError, match="did not accept"):
        channels.send_telegram("x", ENV, FakeOpener(reply={"ok": False}))


def test_ping_retries_then_gives_up_without_the_url() -> None:
    pauses: list[float] = []
    opener = FakeOpener(error=urllib.error.URLError("down"))
    with pytest.raises(channels.AlertDeliveryError, match="3 times") as raised:
        channels.ping("https://hc-ping.com/secret-uuid", opener, pauses.append)
    assert len(opener.requests) == channels.PING_ATTEMPTS
    assert pauses == [channels.PING_PAUSE_S] * (channels.PING_ATTEMPTS - 1)
    assert "secret-uuid" not in str(raised.value)


def test_ping_succeeds_at_the_first_answer() -> None:
    opener = FakeOpener()
    channels.ping("https://hc-ping.com/uuid", opener, lambda s: None)
    assert opener.requests == ["https://hc-ping.com/uuid"]


# --- callbacks ------------------------------------------------------------------------


def fake_ti(task_id: str = "dbt_build", try_number: int = 1, max_tries: int = 0) -> Any:
    return SimpleNamespace(
        dag_id="crypto_pipeline",
        task_id=task_id,
        run_id="scheduled__2026-11-22T15:07:00+00:00",
        try_number=try_number,
        max_tries=max_tries,
        start_date=START,
        log_url=f"http://localhost:8080/dags/crypto_pipeline/runs/r/tasks/{task_id}",
    )


@pytest.fixture()
def target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "target"
    monkeypatch.setattr(dbt_artifacts, "DBT_TARGET", path)
    monkeypatch.setattr(callbacks, "LOG_ROOT", tmp_path / "logs")
    monkeypatch.setattr(callbacks, "LOG_WAIT_S", 0)
    return path


def log_line(
    event: str,
    logger: str = "airflow.task.hooks.airflow.providers.standard.hooks.subprocess.SubprocessHook",
) -> str:
    return json.dumps({"event": event, "logger": logger, "level": "info"})


def test_dbt_failure_message_names_the_check_its_rows_and_the_log(
    target: Path, tmp_path: Path
) -> None:
    write(target, "run_results.json", run_results(FAILED_OHLC, VOLUME_WARN))
    # dbt's own output ends with its summary line: the artifacts say more, so no tail.
    path = callbacks.log_path(fake_ti(), tmp_path / "logs")
    path.parent.mkdir(parents=True)
    path.write_text(log_line("Done. PASS=116 WARN=2 ERROR=1") + "\n", encoding="utf-8")
    exception = RuntimeError("Bash command failed. The command returned a non-zero exit code 1.")
    text = callbacks.task_failure_text({"ti": fake_ti(), "exception": exception})
    assert text.splitlines()[:3] == [
        "❌ crypto_pipeline: dbt_build failed",
        "run scheduled__2026-11-22T15:07:00+00:00, try 1/1",
        "Bash command failed. The command returned a non-zero exit code 1.",
    ]
    assert (
        "dbt build:\nFAIL dbt_utils_expression_is_true_stg_binance__klines_1h_ohlc: 1 rows" in text
    )
    assert "select * from CRYPTO_DWH.DQ.dbt_utils_expression_is_true_stg_ohlc" in text
    assert text.endswith("log: http://localhost:8080/dags/crypto_pipeline/runs/r/tasks/dbt_build")
    assert "last output" not in text and "Done. PASS" not in text


def test_non_dbt_failure_shows_the_last_output_of_the_command(target: Path, tmp_path: Path) -> None:
    ti = fake_ti("load_raw", try_number=2, max_tries=1)
    path = callbacks.log_path(ti, tmp_path / "logs")
    path.parent.mkdir(parents=True)
    lines = [
        log_line("Running command: ['/usr/bin/bash', '-c', '...']"),
        log_line("Output:"),
        log_line("klines: 1 pending batch"),
        log_line("ERROR: klines 20261122T150710Z: 17518 rows, manifest 8759"),
        log_line("not output", logger="task"),
        log_line("Command exited with return code 1"),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    text = callbacks.task_failure_text({"ti": ti, "exception": None})
    assert "try 2/2" in text
    tail = "last output:\nklines: 1 pending batch\nERROR: klines 20261122T150710Z: 17518 rows"
    assert f"{tail}, manifest 8759\n" in text
    assert "not output" not in text and "Running command" not in text


def test_failure_message_without_artifacts_or_log_still_says_what_failed(target: Path) -> None:
    ti = SimpleNamespace(
        dag_id="crypto_pipeline", task_id="extract_klines", run_id="r", try_number=3
    )
    assert (
        callbacks.task_failure_text({"ti": ti})
        == "❌ crypto_pipeline: extract_klines failed\nrun r, try 3"
    )


def test_run_callback_stays_silent_on_task_failures_and_reports_timeouts() -> None:
    dag = SimpleNamespace(dag_id="crypto_pipeline")
    assert callbacks.run_failure_text({"dag": dag, "run_id": "r", "reason": "task_failure"}) is None
    text = callbacks.run_failure_text(
        {"dag": dag, "run_id": "r", "reason": "timed_out", "ti": fake_ti()}
    )
    assert text == "❌ crypto_pipeline: run timed_out\nrun r\nlast task: dbt_build"


class FakeVariable:
    def __init__(self, value: Any = None) -> None:
        self.value = value
        self.sets = 0

    def get(self, key: str, default: Any = None, deserialize_json: bool = False) -> Any:
        assert key == callbacks.WARNING_STATE_VARIABLE and deserialize_json
        return default if self.value is None else self.value

    def set(self, key: str, value: Any, serialize_json: bool = False) -> None:
        assert key == callbacks.WARNING_STATE_VARIABLE and serialize_json
        self.value, self.sets = value, self.sets + 1


@pytest.fixture()
def sent(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    messages: list[str] = []
    monkeypatch.setattr(callbacks, "send_telegram", messages.append)
    return messages


def digest(
    target: Path, monkeypatch: pytest.MonkeyPatch, variable: FakeVariable, *results: dict
) -> None:
    write(target, "run_results.json", run_results(*results))
    monkeypatch.setattr(callbacks, "Variable", variable)
    callbacks.WarningDigestNotifier().notify({"ti": fake_ti()})


def test_first_digest_lists_the_warnings_and_remembers_them(target, monkeypatch, sent) -> None:
    variable = FakeVariable()
    digest(target, monkeypatch, variable, VOLUME_WARN, FAILED_OHLC)
    assert sent == [
        "⚠️ crypto_pipeline: DQ warnings now (first digest, alerts on growth from here)\n"
        "run scheduled__2026-11-22T15:07:00+00:00, dbt_build\n"
        "WARN log_volume_zscore: 25 rows\n  select * from CRYPTO_DWH.DQ.dq_volume_anomaly"
    ]
    assert variable.value == {"log_volume_zscore": 25}


def test_steady_warnings_send_nothing_and_write_nothing(target, monkeypatch, sent) -> None:
    variable = FakeVariable({"log_volume_zscore": 25})
    digest(target, monkeypatch, variable, VOLUME_WARN)
    assert sent == [] and variable.sets == 0


def test_grown_warning_is_sent_with_the_previous_count(target, monkeypatch, sent) -> None:
    variable = FakeVariable({"log_volume_zscore": 24, "funding_gap": 4})
    digest(target, monkeypatch, variable, VOLUME_WARN)
    assert sent[0].splitlines()[2:] == [
        "WARN log_volume_zscore: 24 -> 25 rows",
        "  select * from CRYPTO_DWH.DQ.dq_volume_anomaly",
        "resolved: funding_gap (was 4)",
    ]
    assert variable.value == {"log_volume_zscore": 25}


def test_shrinking_warning_updates_the_state_quietly(target, monkeypatch, sent) -> None:
    variable = FakeVariable({"log_volume_zscore": 30})
    digest(target, monkeypatch, variable, VOLUME_WARN)
    assert sent == [] and variable.value == {"log_volume_zscore": 25}


def test_state_moves_on_only_after_the_message_is_sent(target, monkeypatch) -> None:
    def refuse(text: str) -> None:
        raise channels.AlertDeliveryError("Telegram is unreachable")

    monkeypatch.setattr(callbacks, "send_telegram", refuse)
    variable = FakeVariable({"log_volume_zscore": 24})
    with pytest.raises(channels.AlertDeliveryError):
        digest(target, monkeypatch, variable, VOLUME_WARN)
    assert variable.value == {"log_volume_zscore": 24}


def test_heartbeat_pings_the_configured_url(monkeypatch: pytest.MonkeyPatch) -> None:
    pinged: list[str] = []
    monkeypatch.setattr(callbacks, "ping", pinged.append)
    monkeypatch.setenv("PIPELINE_HEARTBEAT_URL", "https://hc-ping.com/uuid")
    callbacks.HeartbeatNotifier().notify({})
    assert pinged == ["https://hc-ping.com/uuid"]
    monkeypatch.setenv("PIPELINE_HEARTBEAT_URL", "")
    with pytest.raises(channels.AlertDeliveryError, match="PIPELINE_HEARTBEAT_URL"):
        callbacks.HeartbeatNotifier().notify({})
