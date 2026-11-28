"""Tests for the market-context screener: rules, message, once a day, no features yet."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from signals import screener

DAY = date(2026, 11, 28)


def member(symbol: str, **overrides: Any) -> dict[str, Any]:
    row = {
        "decision_date": pd.Timestamp(DAY), "symbol": symbol, "turnover_rank": 10,
        "funding_1d": 0.0001, "funding_z_30d": 0.0, "oi_chg_1d": 0.0, "oi_chg_7d": 0.0,
        "ret_7d": 0.0, "top_position_ls": 1.0, "global_ls": 1.0, "rank_top_position_ls": 0.0,
        "rank_global_ls": 0.0, "btc_close_to_sma50": -0.19, "btc_ret_7d": 0.02,
        "breadth_above_sma20": 0.14,
    }  # fmt: skip
    return row | overrides


def panel(*rows: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def test_rules_pick_their_members_in_sort_order() -> None:
    p = panel(
        member("HOT1", funding_z_30d=3.5),
        member("HOT2", funding_z_30d=4.5),
        member("CALM", funding_z_30d=2.9),
        member("SQZ", funding_1d=-0.0002, funding_z_30d=-2.5, oi_chg_7d=0.3),
    )
    by_title = {r.title: r for r in screener.RULES}
    hot = screener.matches(p, by_title["Funding hot (longs pay)"])
    assert hot["symbol"].tolist() == ["HOT2", "HOT1"]
    squeeze = screener.matches(p, by_title["Short squeeze setup (shorts pile in and pay)"])
    assert squeeze["symbol"].tolist() == ["SQZ"]


def test_a_rule_lists_at_most_rows_per_rule() -> None:
    p = panel(*[member(f"C{i}", oi_chg_7d=0.5 + i / 100) for i in range(8)])
    rule = next(r for r in screener.RULES if r.title == "Open interest spike")
    assert screener.matches(p, rule)["symbol"].tolist() == ["C7", "C6", "C5", "C4", "C3"]


def test_message_says_it_is_no_signal_and_skips_empty_rules() -> None:
    text = screener.message(DAY, panel(member("HOT", funding_z_30d=3.2, global_ls=float("nan"))))
    assert text.startswith("🔎 CONTEXT, not a signal - market of 2026-11-28 (data to 2026-11-27")
    assert "BTC below its 50-day SMA (-19%)" in text
    assert "Funding hot (longs pay):\n  HOT (rank 10): funding z +3.2" in text
    assert "/n/a" in text  # a missing feature is shown, not an error
    assert "Funding cold" not in text
    calm = screener.message(DAY, panel(member("CALM")))
    assert "No member meets a rule today." in calm


def warehouse(tmp_path: Path, days: list[date]) -> Path:
    path = tmp_path / "w.duckdb"
    rows = pd.DataFrame([member("BTCUSDT", funding_z_30d=3.1) | {"decision_date": d} for d in days])
    with duckdb.connect(str(path)) as con:
        con.execute("create schema staging")
        con.register("rows", rows)
        con.execute("create table staging.int_um__features_daily as select * from rows")
    return path


def test_sent_once_a_day_and_not_before_the_features(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: list[str] = []
    monkeypatch.setattr("signals.notify.send", sent.append)
    monkeypatch.setattr(screener, "read_today", lambda w, d: _read(w, d))
    out = tmp_path / "out"
    args = ["--out-dir", str(out), "--nothing-new-exit-code", "99"]

    early = warehouse(tmp_path, [date(2026, 11, 27)])
    assert screener.main([*args, "--warehouse", str(early)], today=lambda: DAY) == 99
    assert sent == [] and not out.exists()

    (tmp_path / "w.duckdb").unlink()
    ready = warehouse(tmp_path, [date(2026, 11, 27), DAY])
    assert screener.main([*args, "--warehouse", str(ready)], today=lambda: DAY) == 0
    assert len(sent) == 1 and "BTCUSDT" in sent[0]
    assert (out / "2026-11-28.txt").read_text(encoding="utf-8") == sent[0] + "\n"
    assert screener.main([*args, "--warehouse", str(ready)], today=lambda: DAY) == 99
    assert len(sent) == 1


def _read(path: Path, day: date) -> pd.DataFrame:
    """The real query, without research.data's ranks (the test table has them already)."""
    with duckdb.connect(str(path), read_only=True) as con:
        return con.execute(
            "select * from staging.int_um__features_daily where decision_date = ?", [day]
        ).df()
