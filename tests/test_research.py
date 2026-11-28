"""The research protocol: embargo, holdout, trade selection, inference, modes."""

from __future__ import annotations

import json
from datetime import date

import numpy as np
import pandas as pd
import pytest

from research import config, data, folds, run, selection, stats, strategies


def daily_panel(days: int, start: str = "2024-12-20") -> pd.DataFrame:
    dates = pd.date_range(start, periods=days, freq="D")
    return pd.DataFrame({"decision_date": dates, "symbol": "AUSDT"})


def test_train_labels_exit_before_the_test_block() -> None:
    panel = daily_panel(30)
    fold = folds.Fold(date(2025, 1, 1), date(2025, 1, 8))
    train, test = folds.split(panel, fold, horizon=7)
    # The last training decision exits on 2024-12-31 (decision + 7 < 2025-01-01).
    train_days = sorted(pd.Timestamp(d).date() for d in train["decision_date"])
    test_days = sorted(pd.Timestamp(d).date() for d in test["decision_date"])
    assert train_days[-1] == date(2024, 12, 24)
    assert test_days[0] == date(2025, 1, 1) and test_days[-1] == date(2025, 1, 7)


def test_folds_are_consecutive_blocks() -> None:
    made = folds.make_folds([date(2022, 1, 1), date(2022, 7, 1)], date(2023, 1, 1))
    assert [(f.test_start, f.test_end) for f in made] == [
        (date(2022, 1, 1), date(2022, 7, 1)),
        (date(2022, 7, 1), date(2023, 1, 1)),
    ]


def test_holdout_rows_and_labels_reaching_it_are_dropped() -> None:
    panel = daily_panel(20, "2025-06-20")
    for h in config.HORIZONS:
        panel[f"nl_{h}"] = 1.0
    kept = data.drop_holdout(panel, date(2025, 7, 1))
    labels = dict(
        zip((pd.Timestamp(d).date() for d in kept["decision_date"]), kept["nl_3"], strict=True)
    )
    assert max(labels) == date(2025, 6, 30)
    # h = 3: 2025-06-27 exits 06-30 (kept), 06-28 exits 07-01 (blanked).
    assert labels[date(2025, 6, 27)] == 1.0
    assert np.isnan(labels[date(2025, 6, 28)])
    assert not kept["nl_14"].notna().to_numpy().any()


def candidates(rows: list[tuple[str, str, str, float]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=["decision_date", "symbol", "side", "score"])
    frame["decision_date"] = pd.to_datetime(frame["decision_date"])
    return frame


def test_one_trade_a_day_and_no_second_position_in_a_symbol() -> None:
    scored = candidates(
        [
            ("2025-01-01", "AUSDT", "long", 0.9),
            ("2025-01-01", "BUSDT", "long", 0.5),
            ("2025-01-02", "AUSDT", "short", 0.8),  # A open until 01-04: blocked
            ("2025-01-02", "BUSDT", "long", 0.1),
            ("2025-01-03", "CUSDT", "long", -0.2),  # not above 0: never a trade
            ("2025-01-04", "AUSDT", "long", 0.3),  # A free again on day 3
        ]
    )
    trades = selection.select_trades(scored, horizon=3)
    assert list(zip(trades["decision_date"].dt.day, trades["symbol"], strict=True)) == [
        (1, "AUSDT"),
        (2, "BUSDT"),
        (4, "AUSDT"),
    ]


def test_block_bootstrap_known_cases() -> None:
    constant = [[0.01], [], [0.01], [0.01]]
    mean, low, high, p = stats.block_bootstrap_mean(constant, 2, 500, 1)
    assert mean == pytest.approx(0.01)
    assert low == pytest.approx(0.01) and high == pytest.approx(0.01)
    assert p == 0.0
    rng = np.random.default_rng(0)
    noise = [[float(v)] for v in rng.normal(0.0, 1.0, 400)]
    _, low, high, p = stats.block_bootstrap_mean(noise, 5, 2000, 2)
    assert low < 0 < high and 0.05 < p < 0.95
    assert np.isnan(stats.block_bootstrap_mean([[], []], 2, 10, 3)[0])


def test_holm_adjustment() -> None:
    adjusted = stats.holm([0.01, 0.04, 0.03, 0.005, float("nan")])
    assert adjusted[:4] == pytest.approx([0.03, 0.06, 0.06, 0.02])
    assert np.isnan(adjusted[4])


def test_ridge_without_penalty_is_least_squares(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "RIDGE_ALPHA", 0.0)
    rng = np.random.default_rng(1)
    x = pd.DataFrame(rng.normal(size=(200, 3)), columns=["a", "b", "c"])
    y = pd.Series(0.5 + x @ np.array([1.0, -2.0, 0.25]) + rng.normal(0, 0.01, 200))
    predict = strategies.fit_ridge(x, y)
    design = np.column_stack([np.ones(200), x.to_numpy()])
    expected = design @ np.linalg.lstsq(design, y.to_numpy(), rcond=None)[0]
    assert np.allclose(predict(x), expected)


def test_composite_weights_follow_the_train_ic() -> None:
    days = pd.date_range("2024-01-01", periods=80, freq="D")
    rng = np.random.default_rng(2)
    rows = []
    for day in days:
        values = rng.normal(size=30)
        for i, v in enumerate(values):
            rows.append({"decision_date": day, "symbol": f"S{i}", "g_7": v})
    train = pd.DataFrame(rows)
    train["rank_ret_7d"] = train.groupby("decision_date")["g_7"].rank(pct=True) - 0.5
    for column in strategies.RANKS:
        if column != "rank_ret_7d":
            train[column] = rng.uniform(-0.5, 0.5, len(train))
    weights = strategies.composite_weights(train, 7)
    assert weights["rank_ret_7d"] == pytest.approx(1.0)
    assert all(abs(w) < 0.5 for c, w in weights.items() if c != "rank_ret_7d")


def test_modes_filter_the_candidates() -> None:
    scored = candidates(
        [
            ("2025-01-01", "AUSDT", "long", 0.9),
            ("2025-01-01", "BTCUSDT", "short", 0.8),
            ("2025-01-02", "AUSDT", "long", 0.7),
            ("2025-01-02", "BUSDT", "short", 0.6),
        ]
    )
    test = pd.DataFrame(
        {"decision_date": pd.to_datetime(["2025-01-01", "2025-01-02"]),
         "btc_close_to_sma50": [0.05, -0.02]}
    )  # fmt: skip
    hedged = strategies.apply_mode(scored, test, "hedged")
    assert "BTCUSDT" not in set(hedged["symbol"])
    regime = strategies.apply_mode(scored, test, "regime")
    assert list(zip(regime["symbol"], regime["side"], strict=True)) == [
        ("AUSDT", "long"),
        ("BUSDT", "short"),
    ]


def test_hedged_trade_adds_beta_times_the_opposite_btc_trade() -> None:
    h = 14
    panel = pd.DataFrame(
        {
            "decision_date": pd.to_datetime(["2025-01-01"]),
            "symbol": ["AUSDT"],
            f"nl_{h}": [0.05], f"ns_{h}": [-0.06], f"mael_{h}": [-0.10], f"maes_{h}": [-0.20],
            f"base_nl_{h}": [0.01], f"base_ns_{h}": [-0.02], "beta_btc_30d": [2.0],
            f"btc_nl_{h}": [0.008], f"btc_ns_{h}": [-0.01],
            f"btc_mael_{h}": [-0.03], f"btc_maes_{h}": [-0.02],
        }
    )  # fmt: skip
    trade = candidates([("2025-01-01", "AUSDT", "long", 1.0)])
    plain = run.evaluate(trade, panel, h)
    assert plain["net"].item() == pytest.approx(0.05)
    assert plain["excess"].item() == pytest.approx(0.04)
    hedged = run.evaluate(trade, panel, h, "hedged")
    assert hedged["net"].item() == pytest.approx(0.05 + 2 * -0.01)
    assert hedged["mae"].item() == pytest.approx(-0.10 + 2 * -0.02)
    assert hedged["exposure"].item() == pytest.approx(3.0)


def test_placebo_keeps_each_days_labels_and_the_features() -> None:
    panel = pd.DataFrame(
        {
            "decision_date": pd.to_datetime(["2025-01-01"] * 3 + ["2025-01-02"] * 3),
            "symbol": list("ABCABC"),
            "ret_1d": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "nl_3": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
        }
    )
    shuffled = run.shuffle_labels_within_day(panel, 7)
    assert shuffled["ret_1d"].tolist() == panel["ret_1d"].tolist()
    for day, group in shuffled.groupby("decision_date"):
        original = panel[panel["decision_date"] == day]["nl_3"]
        assert sorted(group["nl_3"]) == sorted(original)


def test_holdout_is_opened_once(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = tmp_path / "holdout_log.jsonl"
    log.write_text(json.dumps({"version": "lgbm_hedged_h14", "run_id": "x"}) + "\n")
    monkeypatch.setattr(run, "HOLDOUT_LOG", log)
    with pytest.raises(SystemExit):
        run.main(["--holdout", "lgbm_hedged_h14", "--warehouse", str(tmp_path / "none.duckdb")])
    assert run.opened_versions(log) == {"lgbm_hedged_h14"}
