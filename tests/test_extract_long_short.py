"""Tests for extract_long_short: three series, each its own endpoint and landing zone, one run.

The shared algorithm (window, paging, increment) is tested once, in test_futures_stats.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from ingestion import extract_long_short as els
from ingestion import futures_stats
from ingestion.batches import complete_batches
from ingestion.extract_klines import DAY_MS, HOUR_MS
from tests.factories import START_MS, make_long_short_ratios
from tests.fakes import FakeFutures

NOW_MS = START_MS + 40 * DAY_MS + 30 * 60 * 1000  # 00:30: the 00:00 point is published
SKIP = 99
# A different long share per endpoint: a file shows which endpoint filled it.
LONG_SHARE = {
    "topLongShortAccountRatio": "0.7000",
    "topLongShortPositionRatio": "0.6000",
    "globalLongShortAccountRatio": "0.5000",
}


def test_each_series_has_its_own_endpoint_and_landing_zone() -> None:
    # Independent literals: a series pointed at another endpoint or directory would mix two
    # different populations of traders in one RAW table.
    assert [(s.endpoint, s.name) for s in els.SERIES] == [
        ("topLongShortAccountRatio", "top_account_ratio_1h"),
        ("topLongShortPositionRatio", "top_position_ratio_1h"),
        ("globalLongShortAccountRatio", "global_account_ratio_1h"),
    ]


class TickingClock:
    """time.time() that moves one second per call: a run that read the clock per series
    would give its batches different ids."""

    def __init__(self, start_ms: int) -> None:
        self.now_s = start_ms / 1000

    def time(self) -> float:
        self.now_s += 1
        return self.now_s


@pytest.fixture
def offline(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeFutures:
    """main without network or the real landing zone: BTCUSDT, every point of the window."""
    points = {
        endpoint: {"BTCUSDT": make_long_short_ratios("BTCUSDT", START_MS, 40 * 24 + 1, share)}
        for endpoint, share in LONG_SHARE.items()
    }
    fake = FakeFutures(stats=points)
    monkeypatch.setattr(els, "load_contracts", lambda: ["BTCUSDT"])
    monkeypatch.setattr(els, "BinanceFuturesClient", lambda: fake)
    monkeypatch.setattr(els, "time", SimpleNamespace(time=TickingClock(NOW_MS).time))
    monkeypatch.setattr(futures_stats, "LANDING_ZONE_DIR", tmp_path / "data" / "raw")
    monkeypatch.setattr(futures_stats, "PAUSE_S", 0)
    return fake


def test_one_run_writes_a_batch_per_series_under_one_batch_id(
    offline: FakeFutures, tmp_path: Path
) -> None:
    assert els.main(["--nothing-new-exit-code", str(SKIP)]) == 0

    batches = {s.name: complete_batches(tmp_path / "data" / "raw" / s.name) for s in els.SERIES}
    assert [len(b) for b in batches.values()] == [1, 1, 1]
    assert len({b[0].name for b in batches.values()}) == 1  # one as-of moment, one batch id
    for series in els.SERIES:
        (batch_dir,) = batches[series.name]
        points = json.loads((batch_dir / "BTCUSDT.json").read_text(encoding="utf-8"))
        assert {p["longAccount"] for p in points} == {LONG_SHARE[series.endpoint]}
        assert len(points) == 30 * 24  # the whole served window
    assert {call[0] for call in offline.stats_calls} == set(LONG_SHARE)


def test_one_series_with_a_batch_is_enough_to_load(
    monkeypatch: pytest.MonkeyPatch, offline: FakeFutures, tmp_path: Path
) -> None:
    def only_the_first_series(client: Any, series: Any, *args: Any, **kwargs: Any) -> Path | None:
        return tmp_path if series is els.SERIES[0] else None

    monkeypatch.setattr(els, "extract_batch", only_the_first_series)
    assert els.main(["--nothing-new-exit-code", str(SKIP)]) == 0


def test_second_run_in_the_same_hour_has_nothing_new(offline: FakeFutures) -> None:
    assert els.main(["--nothing-new-exit-code", str(SKIP)]) == 0
    calls = len(offline.stats_calls)

    assert els.main(["--nothing-new-exit-code", str(SKIP)]) == SKIP
    assert len(offline.stats_calls) == calls  # every series up to date: no request


def test_window_matches_open_interest() -> None:
    # Same endpoints family, same retention: the served window is not a per-series setting.
    assert futures_stats.WINDOW_DAYS == 30
    assert futures_stats.window_start_ms(NOW_MS) == START_MS + 10 * DAY_MS + HOUR_MS
