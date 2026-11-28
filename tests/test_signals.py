"""Forward test (signals/): the chained log, the late guard, the daily pick, frozen models."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research import config, selection, strategies
from signals import daily, journal, models, notify, registry
from signals import run as signals_run

DAY = registry.FORWARD_START
SYMBOLS = ["BTCUSDT", *[f"C{i:02d}USDT" for i in range(1, 12)]]


def features_day(day: date, symbols: list[str] = SYMBOLS, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    frame = pd.DataFrame({"decision_date": [day] * len(symbols), "symbol": symbols})
    for column in [*config.SYMBOL_FEATURES, *config.MARKET_FEATURES[:-1]]:
        frame[column] = rng.normal(size=len(symbols))
    frame["hours_feature_day"] = 24
    frame["feature_date"] = day - timedelta(days=1)
    frame["history_days"] = 400
    frame["turnover_rank"] = np.arange(1, len(symbols) + 1)
    frame["beta_btc_30d"] = 1.2
    return frame


class TableBooster:
    """predict() returns the score of each row's symbol, in the order of `symbols`."""

    def __init__(self, scores: dict[str, float], symbols: list[str]):
        self.values = np.array([scores.get(s, -1.0) for s in symbols])

    def predict(self, x: pd.DataFrame) -> np.ndarray:
        return self.values[: len(x)]

    def feature_name(self) -> list[str]:
        return [*strategies.RANKS, *config.MARKET_FEATURES]


def fake_model(long: dict[str, float], short: dict[str, float], symbols=SYMBOLS):
    stats = pd.DataFrame({"mean": 0.0, "std": 1.0}, index=config.MARKET_FEATURES)
    return models.FrozenModel(
        key="lgbm_h14",
        block=DAY,
        sha256="f" * 64,
        boosters={"long": TableBooster(long, symbols), "short": TableBooster(short, symbols)},
        stats={"long": stats, "short": stats},
    )


AS_IS, RISK = registry.VERSIONS


# --- registry ---------------------------------------------------------------------------


def test_model_blocks_are_half_years_from_the_start() -> None:
    assert registry.block_start(date(2026, 11, 29)) == date(2026, 11, 29)
    assert registry.block_start(date(2026, 12, 31)) == date(2026, 11, 29)
    assert registry.block_start(date(2027, 1, 1)) == date(2027, 1, 1)
    assert registry.block_start(date(2027, 6, 30)) == date(2027, 1, 1)
    assert registry.block_start(date(2027, 7, 1)) == date(2027, 7, 1)
    assert registry.train_cut(date(2027, 1, 1)) == date(2026, 12, 27)
    with pytest.raises(ValueError):
        registry.block_start(date(2026, 11, 28))


# --- journal ----------------------------------------------------------------------------


def trade(version: str, symbol: str, side: str = "long") -> dict:
    return {"version": version, "status": journal.TRADE, "symbol": symbol, "side": side}


def write(log_dir: Path, day: date, prev: str, decisions=None) -> str:
    decisions = decisions or [trade(v.name, "C01USDT") for v in registry.VERSIONS]
    now = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(minutes=20)
    return journal.write_day(log_dir, day, decisions, prev, now, {})


def test_chain_verifies_and_breaks_on_any_change(tmp_path: Path) -> None:
    h1 = write(tmp_path, DAY, journal.GENESIS)
    h2 = write(tmp_path, DAY + timedelta(1), h1)
    assert journal.verify(tmp_path) == (DAY + timedelta(1), h2)

    first = journal.path_for(tmp_path, DAY)
    original = first.read_bytes()
    first.write_bytes(original.replace(b"C01USDT", b"C02USDT", 1))
    with pytest.raises(journal.JournalError, match="chain broken"):
        journal.verify(tmp_path)
    first.write_bytes(original)
    journal.verify(tmp_path)


def test_a_removed_or_skipped_day_breaks_the_log(tmp_path: Path) -> None:
    h1 = write(tmp_path, DAY, journal.GENESIS)
    write(tmp_path, DAY + timedelta(2), h1)
    with pytest.raises(journal.JournalError, match="jumps"):
        journal.verify(tmp_path)


def test_a_day_is_written_once(tmp_path: Path) -> None:
    write(tmp_path, DAY, journal.GENESIS)
    with pytest.raises(FileExistsError):
        write(tmp_path, DAY, journal.GENESIS)


def test_a_decision_logged_after_the_entry_is_refused_by_verify(tmp_path: Path) -> None:
    late = journal.entry_time(DAY) + timedelta(minutes=1)
    journal.write_day(
        tmp_path, DAY, [trade(v.name, "C01USDT") for v in registry.VERSIONS],
        journal.GENESIS, late, {},
    )  # fmt: skip
    with pytest.raises(journal.JournalError, match="after the entry"):
        journal.verify(tmp_path)


def test_missed_days_may_be_logged_late(tmp_path: Path) -> None:
    late = journal.entry_time(DAY) + timedelta(hours=5)
    journal.write_day(tmp_path, DAY, journal.missed("late"), journal.GENESIS, late, {})
    assert journal.verify(tmp_path)[0] == DAY


def test_every_registered_version_is_in_each_day(tmp_path: Path) -> None:
    write(tmp_path, DAY, journal.GENESIS, [trade(AS_IS.name, "C01USDT")])
    with pytest.raises(journal.JournalError, match="versions"):
        journal.verify(tmp_path)


# --- the daily pick ---------------------------------------------------------------------


def test_hedged_pick_never_btc_and_hedge_is_clipped_beta() -> None:
    feats = features_day(DAY)
    feats.loc[feats["symbol"] == "C03USDT", "beta_btc_30d"] = 7.0
    model = fake_model({"BTCUSDT": 0.9, "C03USDT": 0.5}, {})
    decision = daily.decide(AS_IS, model, feats, [], DAY)
    assert (decision["symbol"], decision["side"]) == ("C03USDT", "long")
    assert decision["hedge_side"] == "short" and decision["hedge_ratio"] == 3.0
    assert decision["exit_time"].startswith("2026-12-13T01:00")

    feats.loc[feats["symbol"] == "C03USDT", "beta_btc_30d"] = np.nan
    assert daily.decide(AS_IS, model, feats, [], DAY)["hedge_ratio"] == 1.0


def test_risk_rules_ban_young_and_illiquid_shorts_only() -> None:
    feats = features_day(DAY)
    feats.loc[feats["symbol"] == "C01USDT", "history_days"] = 179  # young
    feats.loc[feats["symbol"] == "C02USDT", "turnover_rank"] = 51  # illiquid
    model = fake_model({"C02USDT": 0.1}, {"C01USDT": 0.9, "C02USDT": 0.8, "C04USDT": 0.3})
    assert daily.decide(AS_IS, model, feats, [], DAY)["symbol"] == "C01USDT"
    risk = daily.decide(RISK, model, feats, [], DAY)
    assert (risk["symbol"], risk["side"], risk["stop_loss"]) == ("C04USDT", "short", 0.15)

    only_banned = fake_model({}, {"C01USDT": 0.9, "C02USDT": 0.8})
    assert daily.decide(RISK, only_banned, feats, [], DAY)["reason"] == "no_positive_score"
    long_of_young = fake_model({"C01USDT": 0.4}, {})
    assert daily.decide(RISK, long_of_young, feats, [], DAY)["symbol"] == "C01USDT"


def test_open_positions_block_their_symbol_and_cap_the_risk_version() -> None:
    feats = features_day(DAY + timedelta(14))
    model = fake_model({"C01USDT": 0.9, "C02USDT": 0.5}, {})
    open_c01 = [{**trade(AS_IS.name, "C01USDT"), "decision_date": DAY + timedelta(1)}]
    day = DAY + timedelta(14)
    assert daily.decide(AS_IS, model, feats, open_c01, day)["symbol"] == "C02USDT"
    # A trade 14 days back has reached its exit: its symbol is free again.
    closed_c01 = [{**trade(AS_IS.name, "C01USDT"), "decision_date": DAY}]
    assert daily.decide(AS_IS, model, feats, closed_c01, day)["symbol"] == "C01USDT"
    # Another version's position does not block.
    other = [{**trade(RISK.name, "C01USDT"), "decision_date": DAY + timedelta(1)}]
    assert daily.decide(AS_IS, model, feats, other, day)["symbol"] == "C01USDT"

    ten_open = [
        {**trade(RISK.name, f"X{i}USDT"), "decision_date": DAY + timedelta(1 + i)}
        for i in range(10)
    ]
    assert daily.decide(RISK, model, feats, ten_open, day)["reason"] == "max_open"
    assert daily.decide(AS_IS, model, feats, ten_open, day)["status"] == journal.TRADE


def test_day_by_day_picks_equal_the_research_selection() -> None:
    """The forward pick with its own history = research.selection over the same scores."""
    rng = np.random.default_rng(7)
    symbols = SYMBOLS[1:6]
    version = registry.ForwardVersion("plain", config.Version("lgbm", 3))
    rows, history = [], []
    for i in range(40):
        day = DAY + timedelta(i)
        long = dict(zip(symbols, rng.normal(size=5), strict=True))
        short = dict(zip(symbols, rng.normal(size=5), strict=True))
        rows += [(day, s, "long", v) for s, v in long.items()]
        rows += [(day, s, "short", v) for s, v in short.items()]
        decision = daily.decide(
            version, fake_model(long, short, symbols), features_day(day, symbols), history, day
        )
        history.append({**decision, "decision_date": day})
    expected = selection.select_trades(
        pd.DataFrame(rows, columns=["decision_date", "symbol", "side", "score"]), 3
    )
    picked = [(r["decision_date"], r["symbol"], r["side"]) for r in history if "symbol" in r]
    assert picked == list(expected[["decision_date", "symbol", "side"]].itertuples(index=False))
    assert len(picked) > 20


def test_incomplete_features_are_not_ready() -> None:
    feats = features_day(DAY)
    daily.check_day(feats, DAY, len(SYMBOLS))
    partial = feats.assign(hours_feature_day=9)
    with pytest.raises(daily.DataNotReady, match="not complete"):
        daily.check_day(partial, DAY, len(SYMBOLS))
    with pytest.raises(daily.DataNotReady, match="members"):
        daily.check_day(feats.iloc[1:], DAY, len(SYMBOLS))
    with pytest.raises(daily.DataNotReady, match="feature_date"):
        daily.check_day(feats.assign(feature_date=DAY), DAY, len(SYMBOLS))


# --- the run ----------------------------------------------------------------------------


@pytest.fixture
def wired(monkeypatch: pytest.MonkeyPatch):
    """signals.run with features, context and models faked; Telegram recorded."""
    sent: list[str] = []
    state = {"features": features_day(DAY)}
    monkeypatch.setattr(
        signals_run, "read_day", lambda warehouse, day: (state["features"], {"dq": {}}, [])
    )
    monkeypatch.setattr(
        signals_run.models, "ensure", lambda *a, **k: fake_model({"C05USDT": 0.4}, {})
    )
    monkeypatch.setattr(signals_run, "UNIVERSE_SIZE", len(SYMBOLS))
    monkeypatch.setattr(signals_run, "git_commit", lambda: "test")
    monkeypatch.setattr(notify, "send", sent.append)
    return sent, state


def clock(*moments: datetime):
    queue = list(moments)
    return lambda: queue.pop(0) if len(queue) > 1 else queue[0]


def at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


def test_run_logs_today_once_before_the_entry(tmp_path: Path, wired) -> None:
    sent, _ = wired
    argv = ["--signals-dir", str(tmp_path), "--nothing-new-exit-code", "99"]
    assert signals_run.main(argv, clock(at(DAY, 0, 20))) == 0
    document = json.loads(journal.path_for(tmp_path / "log", DAY).read_bytes())
    assert {d["symbol"] for d in document["decisions"]} == {"C05USDT"}
    assert "PAPER TEST" in sent[0] and "C05USDT" in sent[0]
    assert signals_run.main(argv, clock(at(DAY, 3, 10))) == 99
    assert len(sent) == 1


def test_run_after_the_entry_logs_missed_days(tmp_path: Path, wired) -> None:
    sent, _ = wired
    argv = ["--signals-dir", str(tmp_path)]
    assert signals_run.main(argv, clock(at(DAY + timedelta(2), 3, 10))) == 0
    rows = journal.read_all(tmp_path / "log")
    assert {r["status"] for r in rows} == {journal.MISSED}
    assert sorted({r["decision_date"] for r in rows}) == [DAY + timedelta(i) for i in range(3)]
    assert sent == [notify.missed_message([DAY + timedelta(i) for i in range(3)])]


def test_gap_days_are_missed_and_today_is_decided(tmp_path: Path, wired) -> None:
    _, state = wired
    today = DAY + timedelta(2)
    state["features"] = features_day(today)
    assert signals_run.main(["--signals-dir", str(tmp_path)], clock(at(today, 0, 25))) == 0
    rows = journal.read_all(tmp_path / "log")
    status = {r["decision_date"]: r["status"] for r in rows}
    assert status == {DAY: "missed", DAY + timedelta(1): "missed", today: "trade"}
    journal.verify(tmp_path / "log")


def test_run_writes_nothing_when_features_are_incomplete(tmp_path: Path, wired) -> None:
    _, state = wired
    state["features"] = features_day(DAY).assign(hours_feature_day=23)
    assert signals_run.main(["--signals-dir", str(tmp_path)], clock(at(DAY, 0, 20))) == 1
    assert journal.logged_days(tmp_path / "log") == []


def test_a_decision_that_ends_after_the_entry_is_not_logged(tmp_path: Path, wired) -> None:
    argv = ["--signals-dir", str(tmp_path)]
    assert signals_run.main(argv, clock(at(DAY, 0, 59), at(DAY, 1, 0))) == 1
    assert journal.logged_days(tmp_path / "log") == []


def test_run_refuses_a_broken_log(tmp_path: Path, wired) -> None:
    log = tmp_path / "log"
    write(log, DAY, "1" * 64)
    assert signals_run.main(["--signals-dir", str(tmp_path)], clock(at(DAY, 0, 20))) == 1


# --- frozen models ----------------------------------------------------------------------


def labelled_panel(days: int, last_labelled_members: int | None = None) -> pd.DataFrame:
    rng = np.random.default_rng(3)
    start = DAY - timedelta(days=days)
    frames = [features_day(start + timedelta(i), seed=i) for i in range(days)]
    feats = pd.concat(frames, ignore_index=True)
    from research import data

    panel = data.feature_panel(feats)
    for side in ("nl", "ns"):
        panel[f"{side}_14"] = rng.normal(scale=0.05, size=len(panel)) + 0.02 * panel["rank_ret_7d"]
    if last_labelled_members is not None:
        cut = pd.Timestamp(registry.train_cut(DAY))
        newest = panel.loc[panel["decision_date"] + pd.Timedelta(days=14) < cut]
        newest_day = newest["decision_date"].max()
        index = panel.index[panel["decision_date"] == newest_day][last_labelled_members:]
        panel.loc[index, ["nl_14", "ns_14"]] = np.nan
    return panel


def test_model_trains_on_labels_exiting_before_the_cut_and_is_frozen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "LGBM_ROUNDS", 20)
    monkeypatch.setattr(registry, "MIN_LABELLED_MEMBERS", 10)
    monkeypatch.setitem(config.LGBM_PARAMS, "min_data_in_leaf", 20)
    panel = labelled_panel(120)
    model = models.ensure(tmp_path, AS_IS, DAY, lambda: panel)
    path = models.model_path(tmp_path, "lgbm_h14", DAY)
    document = json.loads(path.read_text())
    assert document["last_train_decision"] == str(registry.train_cut(DAY) - timedelta(15))
    assert model.sha256 == models.sha256_file(path)

    scores = model.score(panel.tail(12))
    assert len(scores) == 24 and bool(scores["score"].notna().to_numpy().all())
    # The frozen model scores as the research's own fit on the same rows.
    train = panel.loc[
        panel["decision_date"] + pd.Timedelta(days=14) < pd.Timestamp(registry.train_cut(DAY))
    ]
    expected = strategies.regression(train, panel.tail(12), 14, strategies.fit_lgbm)
    np.testing.assert_allclose(
        scores["score"].to_numpy(), expected["score"].to_numpy(), rtol=0, atol=1e-12
    )
    # A second ensure loads the file: the panel is never read again.
    again = models.ensure(tmp_path, AS_IS, DAY, lambda: pytest.fail("retrained"))
    assert again.sha256 == model.sha256
    pd.testing.assert_frame_equal(again.score(panel.tail(12)), scores)


def test_model_is_not_trained_while_the_newest_labels_are_late(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(registry, "MIN_LABELLED_MEMBERS", 10)
    with pytest.raises(models.ModelError, match="labels are late"):
        models.train(labelled_panel(60, last_labelled_members=5), AS_IS, DAY)


def test_create_once_refuses_an_existing_file(tmp_path: Path) -> None:
    target = tmp_path / "a.json"
    models.create_once(target, b"1")
    with pytest.raises(FileExistsError):
        models.create_once(target, b"2")
    assert target.read_bytes() == b"1"
    assert [p.name for p in tmp_path.iterdir()] == ["a.json"]


def test_day_message_marks_the_paper_test_and_quotes_the_hash() -> None:
    decision = {
        **trade(AS_IS.name, "C05USDT"),
        "horizon_days": 14,
        "exit_time": "2026-12-13T01:00:00+00:00",
        "hedge_symbol": "BTCUSDT",
        "hedge_side": "short",
        "hedge_ratio": 1.25,
        "stop_loss": 0.15,
    }
    text = notify.day_message(DAY, [decision], "ab" * 32, {"members": 100}, [])
    assert text.startswith("📝 PAPER TEST")
    assert "LONG C05USDT + SHORT BTCUSDT x1.25" in text and "15%" in text
    assert "sha256 abababababababab" in text
    unhedged = notify.describe({**decision, "hedge_ratio": 0.0})
    assert "(no BTCUSDT hedge: beta <= 0)" in unhedged and "x0.00" not in unhedged


def test_performance_lines_read_null_counts_as_zero() -> None:
    empty = {"version": "v", "closed_trades": float("nan"), "open_trades": None}
    assert notify.performance_line(empty) == "v: no closed trade yet (0 open)"
    row = {"version": "v", "closed_trades": 12.0, "open_trades": 3, "mean_net": 0.0125,
           "hit_rate": 0.5}  # fmt: skip
    assert notify.performance_line(row) == "v: 12 closed, mean net +1.25%, hit 50%, 3 open"
