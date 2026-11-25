"""Tests for the local environment check: variables and landing zones."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from ingestion import check_environment as ce
from tests.factories import START_MS, make_marked_batch


def full_env() -> dict[str, str]:
    """Every required variable set."""
    return {name: f"value-of-{name}" for name in ce.REQUIRED_VARS}


@pytest.fixture
def landing_zones(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Every dataset's landing zone moved under tmp_path, each with one complete batch."""
    roots = []
    datasets = []
    for dataset in ce.DATASETS:
        root = tmp_path / "raw" / dataset.name
        make_marked_batch(root, "20250101T000000Z", {"BTCUSDT": START_MS}, key="last_ms")
        roots.append(root)
        datasets.append(dataclasses.replace(dataset, landing_dir=root))
    monkeypatch.setattr(ce, "DATASETS", tuple(datasets))
    return roots


def test_no_missing_variables() -> None:
    assert ce.missing_variables({"A": "1", "B": "2"}, ["A", "B"]) == []


def test_empty_variable_counts_as_missing_and_order_is_kept() -> None:
    assert ce.missing_variables({"A": "", "C": "3"}, ["C", "B", "A"]) == ["B", "A"]


def test_required_variables_are_dbt_and_the_warehouse_file() -> None:
    assert ce.REQUIRED_VARS == ("DBT_PROJECT_DIR", "DBT_PROFILES_DIR", "DUCKDB_PATH")


def test_landing_zone_without_directory_is_reported(tmp_path: Path) -> None:
    assert ce.landing_zone_problem(tmp_path / "raw" / "klines_1h") == "directory not found"


def test_landing_zone_with_only_incomplete_batches_is_reported(tmp_path: Path) -> None:
    (tmp_path / "20250101T000000Z").mkdir()  # files but no manifest yet
    assert ce.landing_zone_problem(tmp_path) == "no complete batch"


def test_landing_zone_with_a_complete_batch_is_fine(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, "20250101T000000Z", {"BTCUSDT": START_MS}, key="last_ms")
    assert ce.landing_zone_problem(tmp_path) is None


def test_main_passes_on_a_ready_environment(
    landing_zones: list[Path], capsys: pytest.CaptureFixture[str]
) -> None:
    assert ce.main(full_env()) == 0
    out = capsys.readouterr().out
    assert f"Variables: {len(ce.REQUIRED_VARS)} of {len(ce.REQUIRED_VARS)} set" in out
    assert "Landing zone klines: 1 symbols, newest 2025-01-01 00:00 UTC" in out
    assert "OK: environment is ready" in out


def test_main_names_missing_variables_and_never_prints_values(
    landing_zones: list[Path], capsys: pytest.CaptureFixture[str]
) -> None:
    env = full_env()
    env["DUCKDB_PATH"] = ""
    del env["DBT_PROFILES_DIR"]
    assert ce.main(env) == 1
    out = capsys.readouterr().out
    assert "variable DUCKDB_PATH is not set" in out
    assert "variable DBT_PROFILES_DIR is not set" in out
    assert "value-of-" not in out


def test_main_fails_on_a_landing_zone_without_complete_batches(
    landing_zones: list[Path], capsys: pytest.CaptureFixture[str]
) -> None:
    funding = landing_zones[1]
    for batch_dir in funding.iterdir():
        (batch_dir / "_manifest.json").unlink()
    assert ce.main(full_env()) == 1
    assert "landing zone funding_rate" in capsys.readouterr().out
