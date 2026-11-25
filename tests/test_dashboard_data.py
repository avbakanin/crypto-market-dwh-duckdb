"""Tests for the dashboard's data layer: finding the export, loading it, shaping the views."""

from __future__ import annotations

import json
import os
import shutil
import sys
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest

from dashboard import data
from dashboard.data import PumpFilter, Snapshot, SnapshotError
from tests.factories import EXPORT_ID, EXPORTED_AT, make_mart_rows, utc, write_mart_export


def edit_manifest(export_dir: Path, change: Any) -> None:
    path = export_dir / data.MANIFEST_NAME
    manifest = json.loads(path.read_text(encoding="utf-8"))
    change(manifest)
    path.write_text(json.dumps(manifest), encoding="utf-8")


@pytest.fixture
def snapshot(tmp_path: Path) -> Snapshot:
    write_mart_export(tmp_path)
    found = data.latest_snapshot(tmp_path)
    assert found is not None
    return found


@pytest.fixture
def con(snapshot: Snapshot) -> Iterator[duckdb.DuckDBPyConnection]:
    connection = data.open_snapshot(snapshot)
    yield connection
    connection.close()


# --- finding the newest complete export ---------------------------------------------------


def test_no_directory_or_no_export_is_no_snapshot(tmp_path: Path) -> None:
    assert data.latest_snapshot(tmp_path / "missing") is None
    assert data.latest_snapshot(tmp_path) is None


def test_newest_complete_export_is_read(tmp_path: Path) -> None:
    write_mart_export(tmp_path, "20261124T061030Z")
    write_mart_export(tmp_path, EXPORT_ID)
    snapshot = data.latest_snapshot(tmp_path)
    assert snapshot is not None
    assert snapshot.export_id == EXPORT_ID
    assert snapshot.directory == tmp_path / EXPORT_ID
    assert snapshot.exported_at == EXPORTED_AT
    assert snapshot.tables == (*data.REQUIRED_COLUMNS, data.COVERAGE)
    assert snapshot.rows["fct_pump_events"] == 4


def test_export_in_progress_and_foreign_directories_are_ignored(tmp_path: Path) -> None:
    write_mart_export(tmp_path, EXPORT_ID)
    (tmp_path / "20261124T121030Z").mkdir()  # newer, no manifest yet: still being written
    write_mart_export(tmp_path, "20261124T235959Z-copy")  # a manual copy, sorts last
    (tmp_path / "20261125T000000Z").write_text("a file, not an export", encoding="utf-8")
    snapshot = data.latest_snapshot(tmp_path)
    assert snapshot is not None
    assert snapshot.export_id == EXPORT_ID


def test_broken_newest_export_fails_instead_of_showing_older_data(tmp_path: Path) -> None:
    write_mart_export(tmp_path, "20261124T061030Z")
    newest = write_mart_export(tmp_path, EXPORT_ID)
    (newest / data.MANIFEST_NAME).write_text("{not json", encoding="utf-8")
    with pytest.raises(SnapshotError, match=f"Export {EXPORT_ID}: unreadable manifest"):
        data.latest_snapshot(tmp_path)


@pytest.mark.skipif(
    sys.platform == "win32" or os.geteuid() == 0, reason="POSIX permissions, not as root"
)
def test_unreadable_newest_export_is_an_error_not_a_fallback(tmp_path: Path) -> None:
    """A newer export another user wrote owner-only: name it, do not show the older one."""
    write_mart_export(tmp_path, "20261124T061030Z")
    newest = write_mart_export(tmp_path, EXPORT_ID)
    newest.chmod(0o000)
    try:
        with pytest.raises(SnapshotError, match=f"Export {EXPORT_ID}: no permission"):
            data.latest_snapshot(tmp_path)
    finally:
        newest.chmod(0o755)


def test_manifest_of_another_export_is_rejected(tmp_path: Path) -> None:
    write_mart_export(tmp_path, EXPORT_ID)
    shutil.copytree(tmp_path / EXPORT_ID, tmp_path / "20261124T121030Z")
    with pytest.raises(SnapshotError, match=f"manifest is of export {EXPORT_ID}"):
        data.latest_snapshot(tmp_path)


def test_missing_required_table_is_named(tmp_path: Path) -> None:
    rows = make_mart_rows()
    del rows["mart_pump_profile"]
    write_mart_export(tmp_path, rows=rows)
    with pytest.raises(SnapshotError, match="no table mart_pump_profile"):
        data.latest_snapshot(tmp_path)


def test_missing_required_column_is_named(tmp_path: Path) -> None:
    export_dir = write_mart_export(tmp_path)
    edit_manifest(
        export_dir, lambda m: m["tables"]["fct_pump_events"]["columns"].pop("event_scope")
    )
    with pytest.raises(SnapshotError, match="fct_pump_events has no column event_scope"):
        data.latest_snapshot(tmp_path)


def test_missing_file_is_named(tmp_path: Path) -> None:
    export_dir = write_mart_export(tmp_path)
    (export_dir / "dim_symbol.parquet").unlink()
    with pytest.raises(SnapshotError, match="file dim_symbol.parquet is missing"):
        data.latest_snapshot(tmp_path)


def test_export_without_coverage_is_read_without_it(tmp_path: Path) -> None:
    """Exports made before dq_data_coverage existed: no "data as of", the rest works."""
    rows = make_mart_rows()
    del rows["dq_data_coverage"]
    write_mart_export(tmp_path, rows=rows)
    snapshot = data.latest_snapshot(tmp_path)
    assert snapshot is not None
    assert data.COVERAGE not in snapshot.tables
    with data.open_snapshot(snapshot) as con:
        assert data.coverage(con, snapshot) is None
        assert data.data_as_of(con, snapshot) is None
        assert len(data.pump_events(con, PumpFilter())) == 4


def test_tables_of_other_readers_are_not_loaded(tmp_path: Path) -> None:
    """The analysis tables are in the export too; the dashboard reads its own contract."""
    rows = make_mart_rows()
    rows["mart_pump_signal_check"] = []
    write_mart_export(tmp_path, rows=rows)
    snapshot = data.latest_snapshot(tmp_path)
    assert snapshot is not None
    assert "mart_pump_signal_check" not in snapshot.tables


def test_coverage_without_its_columns_is_rejected(tmp_path: Path) -> None:
    export_dir = write_mart_export(tmp_path)
    edit_manifest(export_dir, lambda m: m["tables"]["dq_data_coverage"]["columns"].pop("data_to"))
    with pytest.raises(SnapshotError, match="dq_data_coverage has no column data_to"):
        data.latest_snapshot(tmp_path)


def test_export_time_without_zone_is_rejected(tmp_path: Path) -> None:
    write_mart_export(tmp_path, exported_at=EXPORTED_AT.replace(tzinfo=None))
    with pytest.raises(SnapshotError, match="no time zone"):
        data.latest_snapshot(tmp_path)


def test_export_time_is_normalized_to_utc(tmp_path: Path) -> None:
    write_mart_export(tmp_path, exported_at=EXPORTED_AT.astimezone(timezone(timedelta(hours=3))))
    snapshot = data.latest_snapshot(tmp_path)
    assert snapshot is not None
    assert snapshot.exported_at == EXPORTED_AT
    assert snapshot.exported_at.utcoffset() == timedelta(0)


# --- loading ------------------------------------------------------------------------------


def test_loaded_connection_cannot_read_files(snapshot: Snapshot) -> None:
    with data.open_snapshot(snapshot) as con:
        path = snapshot.directory / "dim_symbol.parquet"
        with pytest.raises(duckdb.PermissionException):
            con.execute(f"SELECT * FROM read_parquet('{path.as_posix()}')")
        with pytest.raises(duckdb.InvalidInputException):
            con.execute("SET enable_external_access = true")
        with pytest.raises(duckdb.InvalidInputException):  # locked: times stay UTC
            con.execute("SET TimeZone = 'Europe/Belgrade'")


def test_loaded_snapshot_survives_the_prune_of_its_files(snapshot: Snapshot) -> None:
    with data.open_snapshot(snapshot) as con:
        shutil.rmtree(snapshot.directory)
        assert data.scalar(con, "SELECT count(*) FROM fct_pump_events") == 4


def test_unreadable_file_is_a_snapshot_error(snapshot: Snapshot) -> None:
    (snapshot.directory / "dim_symbol.parquet").write_bytes(b"not parquet")
    with pytest.raises(SnapshotError, match="cannot read dim_symbol.parquet"):
        data.open_snapshot(snapshot)


def test_row_count_must_match_the_manifest(tmp_path: Path) -> None:
    export_dir = write_mart_export(tmp_path)
    edit_manifest(export_dir, lambda m: m["tables"]["fct_pump_events"].update(rows=5))
    snapshot = data.latest_snapshot(tmp_path)
    assert snapshot is not None
    with pytest.raises(SnapshotError, match="fct_pump_events has 4 rows, the manifest 5"):
        data.open_snapshot(snapshot)


def test_times_are_utc(con: duckdb.DuckDBPyConnection) -> None:
    events = data.pump_events(con, PumpFilter())
    assert str(events["event_hour"].dt.tz) == "UTC"
    assert events["event_hour"].iloc[-1] == utc("2026-01-10 05:00")


# --- freshness ----------------------------------------------------------------------------


def test_snapshot_goes_stale_after_a_missed_run(snapshot: Snapshot) -> None:
    assert not data.is_stale(snapshot, EXPORTED_AT + data.STALE_AFTER)
    assert data.is_stale(snapshot, EXPORTED_AT + data.STALE_AFTER + timedelta(seconds=1))
    with pytest.raises(ValueError, match="timezone-aware"):
        data.is_stale(snapshot, datetime(2026, 11, 24, 12))


def test_data_as_of_is_the_end_of_the_last_candle(
    con: duckdb.DuckDBPyConnection, snapshot: Snapshot
) -> None:
    """Prices, not the newest dataset: open interest ends an hour later in the fixture."""
    assert data.data_as_of(con, snapshot) == utc("2026-11-24 08:00")


def test_coverage_lists_prices_first_with_their_lag(
    con: duckdb.DuckDBPyConnection, snapshot: Snapshot
) -> None:
    coverage = data.coverage(con, snapshot)
    assert coverage is not None
    assert coverage["dataset"].tolist() == [
        "klines_1h",
        "funding_rate",
        "global_account_ratio_1h",
        "open_interest_1h",
        "top_account_ratio_1h",
        "top_position_ratio_1h",
    ]
    lag = dict(zip(coverage["dataset"], coverage["hours_behind_export"], strict=True))
    assert lag["klines_1h"] == pytest.approx(70 / 60)  # 08:00 -> 09:10:28
    assert lag["open_interest_1h"] == pytest.approx(10 / 60)


# --- pumps --------------------------------------------------------------------------------


def event_ids(con: duckdb.DuckDBPyConnection, **filters: Any) -> list[str]:
    return data.pump_events(con, PumpFilter(**filters))["pump_event_id"].tolist()


def test_all_events_newest_first(con: duckdb.DuckDBPyConnection) -> None:
    assert event_ids(con) == ["e4", "e2", "e3", "e1"]


def test_scope_filter(con: duckdb.DuckDBPyConnection) -> None:
    assert event_ids(con, scope="single_coin") == ["e4", "e1"]
    assert event_ids(con, scope="multi_coin") == ["e2", "e3"]


def test_unknown_scope_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown scope"):
        PumpFilter(scope="market")


def test_symbol_filter(con: duckdb.DuckDBPyConnection) -> None:
    assert event_ids(con, symbols=("BBBUSDT",)) == ["e4", "e3"]
    assert event_ids(con, symbols=("AAAUSDT", "BBBUSDT")) == ["e4", "e2", "e3", "e1"]
    assert event_ids(con, symbols=("CCCUSDT",)) == []


def test_date_range_includes_both_days_whole(con: duckdb.DuckDBPyConnection) -> None:
    day = date(2026, 3, 2)
    assert event_ids(con, date_from=day, date_to=day) == ["e2", "e3"]
    # e4 at 12:00 on the last day is in: the end is a day, not midnight.
    assert event_ids(con, date_from=day, date_to=date(2026, 3, 3)) == ["e4", "e2", "e3"]
    assert event_ids(con, date_from=date(2026, 3, 3)) == ["e4"]
    assert event_ids(con, date_to=date(2026, 3, 1)) == ["e1"]


def test_min_return_includes_the_boundary(con: duckdb.DuckDBPyConnection) -> None:
    assert event_ids(con, min_return=0.08) == ["e2", "e3"]


def test_overlapping_events_can_be_left_out(con: duckdb.DuckDBPyConnection) -> None:
    assert event_ids(con, exclude_overlaps=True) == ["e2", "e3", "e1"]


def test_symbol_options_keep_symbols_without_events(con: duckdb.DuckDBPyConnection) -> None:
    options = data.symbol_options(con)
    assert options[["symbol", "events"]].values.tolist() == [
        ["AAAUSDT", 2],
        ["BBBUSDT", 2],
        ["CCCUSDT", 0],
    ]


def test_event_date_range(con: duckdb.DuckDBPyConnection) -> None:
    assert data.event_date_range(con) == (date(2026, 1, 10), date(2026, 3, 3))
    con.execute("DELETE FROM fct_pump_events")
    assert data.event_date_range(con) is None


def test_summary_counts_days_apart_from_events(con: duckdb.DuckDBPyConnection) -> None:
    summary = data.pump_summary(data.pump_events(con, PumpFilter()))
    assert summary == {
        "events": 4,
        "symbols": 2,
        "days": 3,  # e2 and e3: one market move, one day
        "single_coin": 2,
        "multi_coin": 2,
        "median_return_1h": pytest.approx(0.07),
    }


def test_summary_of_no_events(con: duckdb.DuckDBPyConnection) -> None:
    summary = data.pump_summary(data.pump_events(con, PumpFilter(symbols=("CCCUSDT",))))
    assert summary["events"] == 0
    assert summary["median_return_1h"] is None


def test_events_table_shows_percent_and_basis_points(con: duckdb.DuckDBPyConnection) -> None:
    events = data.pump_events(con, PumpFilter())
    events.loc[events["pump_event_id"] == "e1", "funding_rate_8h_before"] = -0.0001
    table = data.events_table(events).set_index("pump_event_id")
    assert table.loc["e2", "return_1h"] == pytest.approx(12.0)
    assert table.loc["e1", "return_after"] == pytest.approx(1.0)
    assert table.loc["e1", "funding_rate_8h_before"] == pytest.approx(-1.0)
    assert table.loc["e1", "market_breadth"] == 1  # counts stay counts
    # The scope reads as in the filter, not as the raw value.
    assert set(table["scope_label"]) == {"Single coin", "Market move (2+ coins)"}
    assert events.loc[events["pump_event_id"] == "e2", "return_1h"].item() == pytest.approx(0.12)


def test_event_window_is_one_event_in_offset_order(con: duckdb.DuckDBPyConnection) -> None:
    window = data.event_window(con, "e1")
    assert window["offset_hours"].tolist() == [-1, 0, 1]
    assert window["return_since_start"].tolist() == pytest.approx([0, 0.06, 0.07])


# --- profile ------------------------------------------------------------------------------


def test_profile_options_known_first_unknown_last(con: duckdb.DuckDBPyConnection) -> None:
    scopes, metrics = data.profile_options(con)
    assert scopes == ["all", "single_coin", "multi_coin"]
    assert metrics == ["volume_ratio", "funding_premium_8h", "brand_new_metric"]


def test_profile_is_one_scope_and_metric_by_offset(con: duckdb.DuckDBPyConnection) -> None:
    profile = data.pump_profile(con, "single_coin", "volume_ratio")
    assert profile["offset_hours"].tolist() == [-1, 0, 1, 24]
    assert profile["events"].tolist() == [2, 2, 2, 2]
    assert profile["event_median"].tolist() == pytest.approx([1.56, 4.5, 4.1, 1.1])
    assert profile["control_median"].tolist() == pytest.approx([0.7] * 4)
    assert data.pump_profile(con, "multi_coin", "brand_new_metric").empty


def test_profile_is_scaled_to_the_metric_unit(con: duckdb.DuckDBPyConnection) -> None:
    funding = data.scaled_profile(
        data.pump_profile(con, "all", "funding_premium_8h"), "funding_premium_8h"
    )
    assert funding["control_median"].tolist() == pytest.approx([-0.1] * 4)  # basis points
    assert funding["event_p25"].iloc[0] == pytest.approx((1.56 - 0.5) * 10_000)
    assert funding["events"].tolist() == [4, 4, 4, 4]
    volume = data.scaled_profile(data.pump_profile(con, "all", "volume_ratio"), "volume_ratio")
    assert volume["event_median"].tolist() == pytest.approx([1.56, 4.5, 4.1, 1.1])


def test_profile_highlights_pick_the_telling_offsets(con: duckdb.DuckDBPyConnection) -> None:
    profile = data.pump_profile(con, "all", "volume_ratio")
    assert data.profile_highlights(profile)["offset_hours"].tolist() == [-1, 0, 1, 24]
    assert data.profile_highlights(profile, (0,))["offset_hours"].tolist() == [0]


def test_unknown_metric_is_shown_as_it_is() -> None:
    info = data.metric_info("brand_new_metric")
    assert (info.label, info.unit) == ("brand_new_metric", "x")
    assert data.metric_info("funding_premium_8h").scale == 10_000
    assert data.metric_info("return_since_start").scale == 100
    assert data.metric_info("volume_ratio").scale == 1


# --- data quality -------------------------------------------------------------------------


def test_latest_build_counts_only_the_newest_build(con: duckdb.DuckDBPyConnection) -> None:
    assert data.latest_build(con) == {
        "run_started_at": utc("2026-11-24 09:08"),
        "checks": 6,
        "passed": 3,
        "warned": 2,
        "failed": 1,
        "skipped": 0,
    }


def test_selective_build_is_not_the_latest_build(con: duckdb.DuckDBPyConnection) -> None:
    """A mutation test after the pipeline's build: one check is not the pipeline's state."""
    con.execute(
        "INSERT INTO dq_check_results SELECT * REPLACE ('p2' AS invocation_id, "
        "TIMESTAMPTZ '2026-11-24 10:00:00+00' AS run_started_at) "
        "FROM dq_check_results WHERE invocation_id = 'b2' AND status = 'fail'"
    )
    build = data.latest_build(con)
    assert build is not None
    assert (build["run_started_at"], build["checks"]) == (utc("2026-11-24 09:08"), 6)


def test_no_build_in_the_log(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("DELETE FROM dq_check_results WHERE dbt_command = 'build'")
    assert data.latest_build(con) is None


def test_check_status_problems_first_with_readable_names(con: duckdb.DuckDBPyConnection) -> None:
    status = data.check_status(con)
    assert status[["check_label", "status", "failures"]].values.tolist() == [
        ["accepted_values_interval", "fail", 3],
        ["dq_volume_anomaly", "warn", 25],
        ["dq_funding_missing_settlements", "warn", 4],
        ["dq_klines_missing_hours", "pass", 0],
        ["assert_dq_log_matches", "pass", 0],
        ["klines_unit_test", "pass", 0],
        ["not_null_open_price", "pass", 0],
        ["old_check", "pass", 0],
    ]
    in_build = dict(zip(status["check_label"], status["in_latest_build"], strict=True))
    assert not in_build["old_check"]  # only the older build ran it
    assert not in_build["assert_dq_log_matches"]  # runs in dbt test, after the build
    assert in_build["dq_volume_anomaly"]


def test_warn_history_per_full_build_for_checks_with_findings(
    con: duckdb.DuckDBPyConnection,
) -> None:
    history = data.warn_history(con)
    assert history[["check_label", "failures"]].values.tolist() == [
        ["dq_funding_missing_settlements", 4],
        ["dq_funding_missing_settlements", 4],
        ["dq_volume_anomaly", 24],
        ["dq_volume_anomaly", 25],
    ]
    assert history["run_started_at"].tolist()[2:] == [
        utc("2026-11-23 21:08"),
        utc("2026-11-24 09:08"),
    ]
    everything = data.warn_history(con, only_with_findings=False)
    assert set(everything["check_label"]) == {
        "dq_funding_missing_settlements",
        "dq_klines_missing_hours",
        "dq_volume_anomaly",
    }
