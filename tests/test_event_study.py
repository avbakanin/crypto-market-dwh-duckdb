"""Tests for the event study (events, side, base, survival) and the hypothesis ledger."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research import event_study as es
from research import ledger


def panel(days: int = 30) -> pd.DataFrame:
    """Two contracts a day: HOT (signal 1) gains 2 % long on every horizon, COLD loses 1 %."""
    rows = []
    for d in pd.date_range("2024-01-01", periods=days, freq="D"):
        for symbol, signal, ret in (("HOT", 1.0, 0.02), ("COLD", 0.0, -0.01)):
            row = {"decision_date": d, "symbol": symbol, "signal": signal}
            for h in es.HORIZONS:
                row |= {f"nl_{h}": ret, f"ns_{h}": -ret - 0.001, f"mael_{h}": -0.05,
                        f"maes_{h}": -0.40 if symbol == "HOT" else -0.05}  # fmt: skip
            rows.append(row)
    return pd.DataFrame(rows)


def test_events_are_the_rows_of_the_condition_and_the_side_picks_the_label() -> None:
    summary, years = es.study(panel(), "signal > 0.5", "long", horizons=[7], samples=200)
    row = summary.iloc[0]
    assert (row["events"], row["days"]) == (30, 30)
    assert row["mean_net"] == pytest.approx(0.02)
    assert row["hit_rate"] == 1.0
    assert row["excess"] == pytest.approx(0.02 - 0.005)  # the day's base: mean of both
    assert row["ci_low"] == pytest.approx(0.02) and row["ci_high"] == pytest.approx(0.02)
    assert years.to_dict("records") == [
        {"horizon": 7, "year": 2024, "events": 30, "mean_net": pytest.approx(0.02)}
    ]
    short, _ = es.study(panel(), "signal > 0.5", "short", horizons=[7], samples=200)
    assert short.iloc[0]["mean_net"] == pytest.approx(-0.021)
    assert short.iloc[0]["survive_3x"] == 0.0  # MAE -40 % liquidates 3x (limit -32.3 %)


def test_rows_without_a_label_yet_are_not_events_of_that_horizon() -> None:
    p = panel()
    p.loc[p["decision_date"] >= "2024-01-25", "nl_14"] = np.nan  # the last days' labels
    summary, _ = es.study(p, "signal > 0.5", "long", horizons=[1, 14], samples=200)
    assert summary.set_index("horizon")["events"].to_dict() == {1: 30, 14: 24}


def test_no_event_gives_an_empty_row() -> None:
    summary, years = es.study(panel(), "signal > 5", "long", horizons=[3], samples=200)
    assert summary.to_dict("records") == [{"horizon": 3, "events": 0}]
    assert years.empty


def test_survival_uses_the_liquidation_distance() -> None:
    mae = pd.Series([-0.32, -0.33, -0.18, -0.20])
    assert es.survives(mae, 3).tolist() == [True, False, True, True]
    assert es.survives(mae, 5).tolist() == [False, False, True, False]


def test_hypothesis_name_ignores_spacing_but_not_filters() -> None:
    a = es.hypothesis_name("x >  1   and y<2", "long", "")
    assert a == es.hypothesis_name(" x > 1 and y<2 ", "long", "")
    assert a != es.hypothesis_name("x > 1 and y<2", "long", " rank<=50")


def test_ledger_backfills_once_and_counts_distinct_hypotheses(tmp_path: Path) -> None:
    runs, log = tmp_path / "research", tmp_path / "research" / "hypothesis_log.jsonl"
    for run_id, versions in (("R1_walk_forward", ["a", "b"]), ("R2_walk_forward", ["b", "c"])):
        (runs / run_id).mkdir(parents=True)
        (runs / run_id / "manifest.json").write_text(
            json.dumps({"run_id": run_id[:2], "versions": versions}), encoding="utf-8"
        )
    (runs / "R3_placebo").mkdir()
    (runs / "R3_placebo" / "manifest.json").write_text(
        json.dumps({"run_id": "R3", "versions": ["z"]}), encoding="utf-8"
    )
    assert ledger.backfill(runs, log) == 3
    assert ledger.tested(log) == {"a", "b", "c"}
    assert ledger.backfill(runs, log) == 0  # only when there is no ledger yet
    ledger.record(["event:long:x > 1"], "event_study", "R4", log)
    assert len(ledger.tested(log)) == 4
    assert ledger.adjusted_p(0.01, 4) == pytest.approx(0.04)
    assert ledger.adjusted_p(0.4, 4) == 1.0
