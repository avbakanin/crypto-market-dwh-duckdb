"""Smoke tests of the Streamlit page: the script runs headless on a test export
(streamlit.testing), so a broken view fails here and not in the browser."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from tests.factories import EXPORT_ID, make_mart_rows, write_mart_export

APP = str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py")


def run_app(marts_dir: Path, monkeypatch: pytest.MonkeyPatch) -> AppTest:
    monkeypatch.setenv("MARTS_DIR", str(marts_dir))
    app = AppTest.from_file(APP, default_timeout=60)
    app.run()
    assert not app.exception, [e.message for e in app.exception]
    return app


def all_text(app: AppTest) -> str:
    parts = [m.value for m in app.markdown] + [c.value for c in app.caption]
    parts += (
        [e.value for e in app.info] + [e.value for e in app.warning] + [e.value for e in app.error]
    )
    return "\n".join(str(p) for p in parts)


def test_page_shows_the_export_and_every_tab(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_mart_export(tmp_path, exported_at=datetime.now(UTC))
    app = run_app(tmp_path, monkeypatch)
    text = all_text(app)
    assert "Data as of **2026-11-24 08:00 UTC**" in text
    assert EXPORT_ID in text
    assert not app.warning  # fresh export
    labels = ["Pumps", "Profile ±24 h", "Data quality", "Signals (paper test)"]
    assert [t.label for t in app.tabs] == labels
    assert app.metric[0].label == "Events" and app.metric[0].value == "4"
    assert "Checks that need a look" in [s.value for s in app.subheader]


def test_filters_change_the_events(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_mart_export(tmp_path, exported_at=datetime.now(UTC))
    app = run_app(tmp_path, monkeypatch)
    app.radio(key="pumps_scope").set_value("multi_coin").run()
    assert not app.exception
    assert app.metric[0].value == "2"
    assert app.metric[1].value == "1"  # one market move, one day
    assert set(app.dataframe[0].value["scope_label"]) == {"Market move (2+ coins)"}


def test_every_profile_metric_renders(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_mart_export(tmp_path, exported_at=datetime.now(UTC))
    app = run_app(tmp_path, monkeypatch)
    options = next(s for s in app.selectbox if s.label == "Metric").options
    assert len(options) == 3
    for option in options:
        next(s for s in app.selectbox if s.label == "Metric").set_value(option).run()
        assert not app.exception, option


def test_old_export_is_flagged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_mart_export(tmp_path, exported_at=datetime.now(UTC) - timedelta(hours=5))
    app = run_app(tmp_path, monkeypatch)
    assert "The export is 5 hours old" in app.warning[0].value


def test_export_without_coverage_says_so(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rows = make_mart_rows()
    del rows["dq_data_coverage"]
    write_mart_export(tmp_path, exported_at=datetime.now(UTC), rows=rows)
    text = all_text(run_app(tmp_path, monkeypatch))
    assert "Data as of **unknown**" in text
    assert "predates the coverage table" in text


def test_no_export_yet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = run_app(tmp_path, monkeypatch)
    assert "No complete export" in app.info[0].value
    assert not app.tabs


def test_broken_export_is_an_error_not_a_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = make_mart_rows()
    del rows["fct_pump_events"]
    write_mart_export(tmp_path, rows=rows)
    app = run_app(tmp_path, monkeypatch)
    assert "no table fct_pump_events" in app.error[0].value
    assert not app.tabs


def test_signals_tab_shows_results_and_decisions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = make_mart_rows()
    rows["fct_signal_log"] = [
        {"decision_date": date(2026, 11, 29), "version": "fwd_lgbm_hedged_h14", "status": "trade",
         "symbol": "C1USDT", "side": "long", "hedge_symbol": "BTCUSDT", "hedge_side": "short",
         "hedge_ratio": 0.9, "outcome_status": "open"},
        {"decision_date": date(2026, 11, 29), "version": "fwd_lgbm_hedged_risk_h14",
         "status": "no_trade", "reason": "max_open"},
    ]  # fmt: skip
    rows["mart_signal_performance"] = [
        {"version": "fwd_lgbm_hedged_h14", "days_logged": 1, "trades": 1, "open_trades": 1,
         "closed_trades": 0, "days_running": 1, "criteria_met": False},
    ]  # fmt: skip
    write_mart_export(tmp_path, exported_at=datetime.now(UTC), rows=rows)
    app = run_app(tmp_path, monkeypatch)
    assert not app.exception
    frames = [d.value for d in app.dataframe]
    performance = next(f for f in frames if "criteria_met" in f)
    log = next(f for f in frames if "outcome_status" in f)
    assert performance["version"].tolist() == ["fwd_lgbm_hedged_h14"]
    assert log["reason"].tolist() == ["", "max_open"]


def test_signals_tab_of_an_export_before_the_forward_test(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_mart_export(tmp_path, exported_at=datetime.now(UTC))
    app = run_app(tmp_path, monkeypatch)
    assert "This export has no forward-test tables yet." in [i.value for i in app.info]
