"""Tests for extract_funding: hour grid with jitter, per-contract interval, increment, batches."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion import extract_funding as ef
from ingestion.batches import read_manifest
from ingestion.extract_klines import DAY_MS, HOUR_MS
from tests.factories import START_MS, make_funding, make_fundings, make_marked_batch
from tests.fakes import FakeFutures

LAG = ef.PUBLISH_LAG_MS
DAYS = 400
NOW_MS = START_MS + DAYS * DAY_MS + HOUR_MS  # 01:00: the 00:00 settlement is published
EARLIER_BATCH = "20250101T000000Z"  # an earlier run's batch id, before NOW_MS


def at(hours: int) -> int:
    """The hour `hours` after START_MS (2025-01-01 00:00 UTC, on every funding grid)."""
    return START_MS + hours * HOUR_MS


BTC = make_fundings("BTCUSDT", START_MS, DAYS * 3 + 1)  # 8 h, through NOW's 00:00
ENA = make_fundings("ENAUSDT", START_MS, DAYS * 6 + 1, interval_hours=4)


def fake(**overrides: list[dict]) -> FakeFutures:
    funding = {"BTCUSDT": BTC, "ENAUSDT": ENA, **overrides}
    return FakeFutures(funding, intervals={"ENAUSDT": 4})  # BTCUSDT unlisted: default 8 h


@pytest.fixture(autouse=True)
def no_pause(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ef, "PAUSE_S", 0)


def times(batch_dir: Path, symbol: str) -> list[int]:
    records = json.loads((batch_dir / f"{symbol}.json").read_text(encoding="utf-8"))
    return [r["fundingTime"] for r in records]


# --- grid arithmetic --------------------------------------------------------------------------


def test_listed_interval_is_used_and_unlisted_contract_defaults_to_8h() -> None:
    info = FakeFutures(intervals={"ENAUSDT": 4}).funding_info()
    assert ef.funding_intervals(info, ["ENAUSDT", "BTCUSDT"]) == {"ENAUSDT": 4, "BTCUSDT": 8}


@pytest.mark.parametrize(
    ("now_ms", "settled"),
    [(at(16) + LAG, at(16)), (at(16) + LAG - 1, at(8))],
    ids=["lag-passed", "still-in-lag"],
)
def test_settlement_counts_once_its_publication_lag_has_passed(now_ms: int, settled: int) -> None:
    assert ef.last_settled_ms(now_ms, 8) == settled


@pytest.mark.parametrize(("interval", "expected"), [(8, 1095), (4, 2190)])
def test_full_year_window_expects_one_settlement_per_interval(interval: int, expected: int) -> None:
    window_start = NOW_MS - ef.HISTORY_DAYS * DAY_MS  # 01:00: first settlement is the next one
    assert ef.expected_settlements(window_start, NOW_MS, interval) == expected


def test_expected_counts_the_marks_own_settlement_despite_its_jitter() -> None:
    # Increment starts at the mark (16:00:00.026) and re-reads it: 16:00 and 00:00.
    assert ef.expected_settlements(at(16) + 26, at(24) + LAG, 8) == 2


def test_start_is_the_mark_itself_or_the_window_for_a_new_contract() -> None:
    window_start = NOW_MS - ef.HISTORY_DAYS * DAY_MS
    assert ef.start_ms_for(None, NOW_MS) == window_start
    assert ef.start_ms_for(at(9000) + 5, NOW_MS) == at(9000) + 5
    assert ef.start_ms_for(START_MS, NOW_MS) == window_start  # mark older than the window


def test_a_new_contracts_window_starts_on_the_hour_it_expects() -> None:
    # now at 16:45: the window starts 16:00, so the 16:00:00.003 settlement is fetched
    # and counted alike (an unaligned start missed it yet expected it).
    now = at(9000) + 45 * 60_000
    start = ef.start_ms_for(None, now, history_days=1)
    assert start == at(9000 - 24)
    settled = [at(h) + 3 for h in range(9000 - 24, 9001) if h % 8 == 0 and at(h) + 3 >= start]
    assert len(settled) == ef.expected_settlements(start, now + LAG, 8)


# --- check_funding ----------------------------------------------------------------------------


def test_jitter_is_neither_a_gap_nor_a_duplicate() -> None:
    assert ef.check_funding(make_fundings("BTCUSDT", START_MS, 24), 8) == {
        "rows": 24,
        "duplicates": 0,
        "gaps": 0,
    }


def test_two_records_in_one_hour_are_a_duplicate() -> None:
    records = [make_funding("BTCUSDT", at(8)), make_funding("BTCUSDT", at(8) + 5)]
    assert ef.check_funding(records, 8)["duplicates"] == 1


def test_skipped_settlement_is_one_gap_on_the_contracts_interval() -> None:
    records = make_fundings("ENAUSDT", START_MS, 12, interval_hours=4)
    del records[5]
    assert ef.check_funding(records, 4)["gaps"] == 1
    assert ef.check_funding(make_fundings("ENAUSDT", START_MS, 12, 4), 8)["gaps"] == 11


def test_unsorted_records_are_not_gaps() -> None:
    records = make_fundings("BTCUSDT", START_MS, 6)[::-1]
    assert ef.check_funding(records, 8)["gaps"] == 0


# --- fetch_funding ----------------------------------------------------------------------------


def test_pages_forward_from_just_after_the_last_funding_time() -> None:
    records = make_fundings("BTCUSDT", START_MS, 2500)
    source = FakeFutures({"BTCUSDT": records})

    got = ef.fetch_funding(source, "BTCUSDT", START_MS, records[-1]["fundingTime"])

    assert got == records
    assert source.funding_calls == [
        ("BTCUSDT", START_MS),
        ("BTCUSDT", records[999]["fundingTime"] + 1),
        ("BTCUSDT", records[1999]["fundingTime"] + 1),
    ]


def test_partial_page_ends_paging_without_an_extra_request() -> None:
    source = FakeFutures({"BTCUSDT": make_fundings("BTCUSDT", START_MS, 1500)})
    ef.fetch_funding(source, "BTCUSDT", START_MS, NOW_MS)
    assert len(source.funding_calls) == 2


def test_record_settled_after_the_as_of_moment_is_dropped() -> None:
    late = make_funding("BTCUSDT", at(8) + 3)  # settled 2 ms after the run's as-of moment
    source = FakeFutures({"BTCUSDT": [make_funding("BTCUSDT", at(0)), late]})
    assert ef.fetch_funding(source, "BTCUSDT", START_MS, at(8) + 1) == [
        make_funding("BTCUSDT", at(0))
    ]


# --- extract_batch: first run -----------------------------------------------------------------


def test_first_run_writes_each_contract_with_its_interval_and_stats(tmp_path: Path) -> None:
    batch_dir = ef.extract_batch(fake(), ["BTCUSDT", "ENAUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    manifest = read_manifest(batch_dir)
    assert manifest["interval"] == "funding"
    assert manifest["symbols"] == {
        "BTCUSDT": {
            "rows": 1095,
            "duplicates": 0,
            "gaps": 0,
            "expected": 1095,
            "interval_hours": 8,
            "last_ms": BTC[-1]["fundingTime"],
        },
        "ENAUSDT": {
            "rows": 2190,
            "duplicates": 0,
            "gaps": 0,
            "expected": 2190,
            "interval_hours": 4,
            "last_ms": ENA[-1]["fundingTime"],
        },
    }
    assert json.loads((batch_dir / "BTCUSDT.json").read_text(encoding="utf-8")) == BTC[-1095:]


def test_settlement_skipped_by_the_platform_is_a_dq_issue(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    skipped = [r for i, r in enumerate(ENA) if i != len(ENA) - 100]  # like 2026-06-24 04:00
    batch_dir = ef.extract_batch(fake(ENAUSDT=skipped), ["ENAUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    stats = read_manifest(batch_dir)["symbols"]["ENAUSDT"]
    assert (stats["rows"], stats["expected"], stats["gaps"]) == (2189, 2190, 1)
    assert "with DQ issues: 1 ['ENAUSDT']" in capsys.readouterr().out


def test_existing_batch_is_refused_before_any_request(tmp_path: Path) -> None:
    (tmp_path / "20260205T010000Z").mkdir(parents=True)  # batch id of NOW_MS
    source = fake()
    with pytest.raises(FileExistsError):
        ef.extract_batch(source, ["BTCUSDT"], NOW_MS, tmp_path)
    assert (source.info_calls, source.funding_calls) == (0, [])


# --- extract_batch: increments ----------------------------------------------------------------


def test_increment_starts_at_the_mark_and_re_reads_it(tmp_path: Path) -> None:
    mark = BTC[-4]["fundingTime"]  # 3 settlements since
    make_marked_batch(tmp_path, EARLIER_BATCH, {"BTCUSDT": mark}, key="last_ms")
    source = fake()

    batch_dir = ef.extract_batch(source, ["BTCUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    assert source.funding_calls == [("BTCUSDT", mark)]
    assert times(batch_dir, "BTCUSDT") == [r["fundingTime"] for r in BTC[-4:]]
    stats = read_manifest(batch_dir)["symbols"]["BTCUSDT"]
    assert (stats["rows"], stats["expected"]) == (4, 4)


def test_up_to_date_contract_costs_no_request(tmp_path: Path) -> None:
    marks = {"BTCUSDT": BTC[-1]["fundingTime"], "ENAUSDT": ENA[-2]["fundingTime"]}
    make_marked_batch(tmp_path, EARLIER_BATCH, marks, key="last_ms")
    source = fake()

    batch_dir = ef.extract_batch(source, ["BTCUSDT", "ENAUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    assert source.funding_calls == [("ENAUSDT", ENA[-2]["fundingTime"])]


def test_nothing_new_writes_no_batch(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    marks = {"BTCUSDT": BTC[-1]["fundingTime"], "ENAUSDT": ENA[-1]["fundingTime"]}
    make_marked_batch(tmp_path, EARLIER_BATCH, marks, key="last_ms")
    source = fake()

    assert ef.extract_batch(source, ["BTCUSDT", "ENAUSDT"], NOW_MS, tmp_path) is None
    assert source.funding_calls == []
    assert [p.name for p in tmp_path.iterdir()] == [EARLIER_BATCH]
    assert "Nothing new" in capsys.readouterr().out


def test_settlement_within_its_publication_lag_is_not_requested(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, EARLIER_BATCH, {"BTCUSDT": BTC[-2]["fundingTime"]}, key="last_ms")
    source = fake()
    in_lag = START_MS + DAYS * DAY_MS + LAG - 1  # 00:00 is settled but may not be published yet

    assert ef.extract_batch(source, ["BTCUSDT"], in_lag, tmp_path) is None
    assert source.funding_calls == []


def test_contract_without_new_settlements_is_left_out_and_reported(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    halted = BTC[:-3]  # the last 3 settlements never came
    make_marked_batch(
        tmp_path, EARLIER_BATCH, {"BTCUSDT": halted[-1]["fundingTime"]}, key="last_ms"
    )

    assert ef.extract_batch(fake(BTCUSDT=halted), ["BTCUSDT"], NOW_MS, tmp_path) is None
    assert "BTCUSDT       WARNING: no new settlements, expected 3" in capsys.readouterr().out


def test_new_contract_gets_the_full_window_while_others_continue(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, EARLIER_BATCH, {"BTCUSDT": BTC[-2]["fundingTime"]}, key="last_ms")
    source = fake()

    batch_dir = ef.extract_batch(source, ["BTCUSDT", "ENAUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    assert source.funding_calls[0] == ("BTCUSDT", BTC[-2]["fundingTime"])
    assert source.funding_calls[1] == ("ENAUSDT", NOW_MS - ef.HISTORY_DAYS * DAY_MS)
    assert read_manifest(batch_dir)["symbols"]["ENAUSDT"]["rows"] == 2190
