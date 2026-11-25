"""Tests for the pump findings report: the day bootstrap, the summaries, the export contract."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from analysis import pump_findings as pf
from dashboard.data import SnapshotError
from ingestion.export_marts import EXPORTS
from tests.factories import EXPORT_ID, make_mart_rows, write_mart_export

D1, D2, D3 = date(2026, 3, 1), date(2026, 3, 2), date(2026, 3, 3)


def excess_row(event: str, day: date, value: float, **rest: Any) -> dict[str, Any]:
    """A row of fct_pump_event_excess joined with its event; excess follows the control
    median unless given."""
    control_median = rest.get("control_median", 1.0)
    row = {
        "pump_event_id": event,
        "offset_hours": -1,
        "metric": "volume_ratio",
        "value": value,
        "control_hours": 30,
        "control_median": 1.0,
        "control_share_above_zero": 1.0,
        "control_share_below_zero": 0.0,
        "excess": None if control_median is None else value - control_median,
        "event_scope": "single_coin",
        "event_day": day,
    }
    row.update(rest)
    return row


# --- the day bootstrap --------------------------------------------------------------------


def test_bootstrap_resamples_whole_days_not_rows() -> None:
    """Day A: ten pumps at 0, day B: one at 10. Resampling days picks B twice in a quarter
    of the replicates (median 10); resampling rows almost never gives 6 of 11 at 10."""
    days = ["A"] * 10 + ["B"]
    values = np.array([0.0] * 10 + [10.0])
    est = pf.day_bootstrap(days, lambda i: float(np.median(values[i])), n_boot=400)
    assert (est.value, est.low, est.high) == (0.0, 0.0, 10.0)


def test_interval_is_the_middle_95_percent() -> None:
    """40 days, half with a 1: the resampled sum is Binomial(40, 1/2), whose 2.5 % and
    97.5 % points are 14 and 26 (5 % and 95 %: 15 and 25)."""
    values = np.array([1.0, 0.0] * 20)
    est = pf.day_bootstrap(list(range(40)), lambda i: float(values[i].sum()))
    assert (est.value, est.low, est.high) == (20.0, 14.0, 26.0)


def test_one_day_gives_an_interval_of_zero_width() -> None:
    values = np.array([1.0, 2.0, 3.0])
    est = pf.day_bootstrap(["A"] * 3, lambda i: float(np.median(values[i])), n_boot=200)
    assert (est.value, est.low, est.high) == (2.0, 2.0, 2.0)


def test_bootstrap_is_reproducible_by_seed() -> None:
    days = list("ABCDEFGH")
    values = np.arange(8.0)

    def run(seed: int) -> pf.Estimate:
        return pf.day_bootstrap(days, lambda i: float(values[i].mean()), 300, seed)

    assert run(1) == run(1)
    assert run(1) != run(2)
    assert run(1).low < run(1).value < run(1).high


def test_no_rows_is_no_estimate() -> None:
    est = pf.day_bootstrap([], lambda i: 0.0)
    assert np.isnan(est.value) and np.isnan(est.low) and np.isnan(est.high)


# --- per-event excess ---------------------------------------------------------------------


def frame(*rows: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def test_excess_line_reads_one_metric_and_offset() -> None:
    x = frame(
        excess_row("e1", D1, 3.0),
        excess_row("e2", D1, 2.0, control_median=0.5),
        excess_row("e3", D2, 5.0),
        excess_row("e4", D2, 9.0, excess=None, control_median=None, control_hours=0),
        excess_row("e1", D1, 99.0, offset_hours=0),
        excess_row("e1", D1, 99.0, metric="taker_buy_share"),
    )
    line = pf.excess_line(x, "volume_ratio", -1, n_boot=200)
    # e4 has no control value: it cannot be compared and is left out.
    assert (line.events, line.days) == (3, 2)
    assert line.event_median == 3.0  # 2, 3, 5
    assert line.control_median == 1.0  # 0.5, 1, 1
    assert line.excess.value == 2.0  # 1.5, 2, 4


def test_excess_line_of_a_scope() -> None:
    x = frame(
        excess_row("e1", D1, 3.0),
        excess_row("e2", D2, 7.0, event_scope="multi_coin"),
    )
    assert pf.excess_line(x, "volume_ratio", -1, "multi_coin", 200).event_median == 7.0
    assert pf.excess_line(x, "volume_ratio", -1, "all", 200).events == 2


def test_share_line_counts_zero_on_neither_side() -> None:
    x = frame(
        excess_row("e1", D1, 0.01, control_share_above_zero=0.5),
        excess_row("e2", D2, -0.02, control_share_above_zero=0.5),
        excess_row("e3", D3, 0.0, control_share_above_zero=0.25),
    )
    line = pf.share_line(x, "volume_ratio", -1, "above", n_boot=200)
    assert line.event_share == pytest.approx(1 / 3)
    assert line.control_share == pytest.approx(1.25 / 3)
    assert line.difference.value == pytest.approx((0.5 - 0.5 - 0.25) / 3)


def test_share_line_below_zero() -> None:
    x = frame(
        excess_row("e1", D1, -0.0001, control_share_below_zero=0.7),
        excess_row("e2", D2, 0.0, control_share_below_zero=0.7),
    )
    line = pf.share_line(x, "volume_ratio", -1, "below", n_boot=200)
    assert (line.event_share, line.difference.value) == pytest.approx((0.5, -0.2))
    with pytest.raises(ValueError, match="side"):
        pf.share_line(x, "volume_ratio", -1, "under")


# --- the next-hour check ------------------------------------------------------------------


def signal_frame() -> pd.DataFrame:
    return frame(
        {"rule": "every_hour", "signal_date": D1, "signal_hours": 100, "followed_by_pump": 2},
        {"rule": "every_hour", "signal_date": D2, "signal_hours": 100, "followed_by_pump": 0},
        {"rule": "volume_2x", "signal_date": D1, "signal_hours": 10, "followed_by_pump": 1},
    )


def test_signal_lines_precision_lift_and_recall() -> None:
    base, rule = pf.signal_lines(signal_frame(), n_boot=200)
    assert (base.rule, base.hours, base.pumps, base.precision) == ("every_hour", 200, 2, 0.01)
    assert base.lift.value == 1.0 and base.recall == 1.0
    assert (rule.rule, rule.hours, rule.pumps) == ("volume_2x", 10, 1)
    assert rule.precision == pytest.approx(0.1)
    assert rule.lift.value == pytest.approx(10.0)  # against 2 / 200, not 2 / 100
    assert rule.recall == pytest.approx(0.5)


def test_signal_check_needs_the_base_rule() -> None:
    with pytest.raises(ValueError, match="every_hour"):
        pf.signal_lines(signal_frame().query("rule != 'every_hour'"))


# --- the export and the command -----------------------------------------------------------


def analysis_rows() -> dict[str, list[dict[str, Any]]]:
    """The dashboard's small export (events e1-e4, coverage) plus the per-event tables."""
    rows = make_mart_rows()
    rows["fct_pump_event_excess"] = [
        {
            "pump_event_id": e,
            "offset_hours": -1,
            "metric": "volume_ratio",
            "value": v,
            "control_hours": 30,
            "control_median": 0.7,
            "control_share_above_zero": 1,
            "control_share_below_zero": 0,
            "excess": v - 0.7,
        }
        for e, v in (("e1", 1.5), ("e2", 2.0), ("e3", 1.2), ("e4", 0.9))
    ]
    rows["mart_pump_signal_check"] = [
        {"rule": r, "signal_date": d, "signal_hours": h, "followed_by_pump": p}
        for r, d, h, p in (
            ("every_hour", D1, 100, 2),
            ("every_hour", D2, 100, 0),
            ("volume_2x", D1, 10, 1),
        )
    ]
    return rows


def test_report_from_the_newest_export(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    write_mart_export(tmp_path, rows=analysis_rows())
    assert pf.main(["--marts", str(tmp_path), "--boot", "100"]) == 0
    report = capsys.readouterr().out
    assert f"# Pump findings: export {EXPORT_ID}" in report
    assert "Data to 2026-11-24 08:00 UTC" in report
    assert "4 pumps" in report and "on 2 coins over 3 days, 2026-01-10 … 2026-03-03." in report
    # Volume an hour before: medians 1.5/2.0/1.2/0.9 -> 1.35, excess 0.65.
    assert "| Volume vs 7-day baseline | -1 h | 4 | 3 | 1.35× | 0.70× | +0.65 [" in report
    assert "| volume_2x | 10 | 1 | 10.00% | 10.0× [" in report


def test_export_without_the_per_event_tables_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """Exports made before the analysis tables existed: an error naming them."""
    write_mart_export(tmp_path)
    assert pf.main(["--marts", str(tmp_path)]) == 1
    assert "no table fct_pump_event_excess, mart_pump_signal_check" in capsys.readouterr().err


def test_no_export_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(SnapshotError, match="No complete export"):
        pf.load(tmp_path)


def test_too_few_resamples_are_refused(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        pf.main(["--marts", str(tmp_path), "--boot", "99"])
    assert exc.value.code == 2


def test_the_export_writes_every_table_the_analysis_reads() -> None:
    assert set(pf.CONTRACT) <= {export.name for export in EXPORTS}
