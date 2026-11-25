"""Tests for the local environment check: variables, pipeline mode, warehouse and landing zones."""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Callable
from pathlib import Path

import pytest

from ingestion import check_environment as ce
from tests.factories import START_MS, make_marked_batch


@pytest.fixture
def warehouse_dir(tmp_path: Path) -> Path:
    path = tmp_path / "warehouse"
    path.mkdir()
    return path


def full_env(warehouse_dir: Path, mode: str = "follow") -> dict[str, str]:
    """Every required variable set; the warehouse file in an existing directory."""
    env = {name: f"value-of-{name}" for name in ce.REQUIRED_VARS}
    env["DUCKDB_PATH"] = str(warehouse_dir / "crypto_dwh.duckdb")
    env["PIPELINE_MODE"] = mode
    return env


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


def deny(monkeypatch: pytest.MonkeyPatch, denied: Callable[[Path], bool], mode: int) -> None:
    """os.access says no to `mode` on the paths `denied` picks (chmod does not work on Windows)."""
    real = os.access

    def access(path: os.PathLike[str] | str, wanted: int, **kwargs: object) -> bool:
        if wanted == mode and denied(Path(path)):
            return False
        return real(path, wanted)

    monkeypatch.setattr(ce.os, "access", access)


def under(root: Path) -> Callable[[Path], bool]:
    return lambda path: path == root or root in path.parents


# --- variables ---------------------------------------------------------------------------------


def test_no_missing_variables() -> None:
    assert ce.missing_variables({"A": "1", "B": "2"}, ["A", "B"]) == []


def test_empty_variable_counts_as_missing_and_order_is_kept() -> None:
    assert ce.missing_variables({"A": "", "C": "3"}, ["C", "B", "A"]) == ["B", "A"]


def test_required_variables_are_dbt_the_warehouse_file_and_the_mode() -> None:
    assert ce.REQUIRED_VARS == (
        "DBT_PROJECT_DIR",
        "DBT_PROFILES_DIR",
        "DUCKDB_PATH",
        "PIPELINE_MODE",
    )


# --- one landing zone --------------------------------------------------------------------------


@pytest.mark.parametrize("writable", [False, True])
def test_landing_zone_without_directory_is_reported(tmp_path: Path, writable: bool) -> None:
    assert (
        ce.landing_zone_problem(tmp_path / "raw" / "klines_1h", writable) == "directory not found"
    )


def test_landing_zone_with_only_incomplete_batches_is_reported(tmp_path: Path) -> None:
    (tmp_path / "20250101T000000Z").mkdir()  # files but no manifest yet
    assert ce.landing_zone_problem(tmp_path, writable=False) == "no complete batch"


def test_landing_zone_with_a_complete_batch_is_fine(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, "20250101T000000Z", {"BTCUSDT": START_MS}, key="last_ms")
    assert ce.landing_zone_problem(tmp_path, writable=True) is None


def test_a_read_only_landing_zone_serves_a_follower_but_not_an_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    make_marked_batch(tmp_path, "20250101T000000Z", {"BTCUSDT": START_MS}, key="last_ms")
    deny(monkeypatch, under(tmp_path), os.W_OK)
    assert ce.landing_zone_problem(tmp_path, writable=False) is None
    assert ce.landing_zone_problem(tmp_path, writable=True) == "directory not writable"


def test_an_unreadable_landing_zone_serves_no_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    make_marked_batch(tmp_path, "20250101T000000Z", {"BTCUSDT": START_MS}, key="last_ms")
    deny(monkeypatch, under(tmp_path), os.R_OK)
    assert ce.landing_zone_problem(tmp_path, writable=False) == "directory not readable"


# --- main ----------------------------------------------------------------------------------------


@pytest.mark.parametrize(("mode", "access"), [("follow", "read only"), ("own", "read and write")])
def test_main_passes_on_a_ready_environment(
    landing_zones: list[Path],
    warehouse_dir: Path,
    capsys: pytest.CaptureFixture[str],
    mode: str,
    access: str,
) -> None:
    assert ce.main(full_env(warehouse_dir, mode)) == 0
    out = capsys.readouterr().out
    assert f"Variables: {len(ce.REQUIRED_VARS)} of {len(ce.REQUIRED_VARS)} set" in out
    assert f"Pipeline mode: {mode} (landing zone: {access})" in out
    assert "Landing zone klines: 1 symbols, newest 2025-01-01 00:00 UTC" in out
    assert "OK: environment is ready" in out


def test_main_names_missing_variables_and_never_prints_values(
    landing_zones: list[Path], warehouse_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env = full_env(warehouse_dir)
    env["DBT_PROJECT_DIR"] = ""
    del env["DBT_PROFILES_DIR"]
    assert ce.main(env) == 1
    out = capsys.readouterr().out
    assert "variable DBT_PROJECT_DIR is not set" in out
    assert "variable DBT_PROFILES_DIR is not set" in out
    assert "value-of-" not in out


def test_main_refuses_a_mode_it_does_not_know_and_says_which_ones_it_does(
    landing_zones: list[Path], warehouse_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert ce.main(full_env(warehouse_dir, mode="owner")) == 1
    out = capsys.readouterr().out
    assert "PIPELINE_MODE must be one of follow, own" in out


def test_main_an_unset_mode_is_reported_once(
    landing_zones: list[Path], warehouse_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env = full_env(warehouse_dir)
    del env["PIPELINE_MODE"]
    assert ce.main(env) == 1
    out = capsys.readouterr().out
    assert "variable PIPELINE_MODE is not set" in out
    assert "must be one of" not in out


def test_main_follow_accepts_the_original_read_only_landing_zone(
    landing_zones: list[Path],
    warehouse_dir: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deny(monkeypatch, under(tmp_path / "raw"), os.W_OK)
    assert ce.main(full_env(warehouse_dir, "follow")) == 0


def test_main_own_needs_to_write_the_landing_zone(
    landing_zones: list[Path],
    warehouse_dir: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    deny(monkeypatch, under(tmp_path / "raw"), os.W_OK)
    assert ce.main(full_env(warehouse_dir, "own")) == 1
    assert "landing zone klines" in capsys.readouterr().out


def test_main_fails_without_the_warehouse_directory(
    landing_zones: list[Path], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert ce.main(full_env(tmp_path / "no-such-dir")) == 1
    assert "warehouse directory" in capsys.readouterr().out


def test_main_fails_on_a_read_only_warehouse_directory_even_when_following(
    landing_zones: list[Path],
    warehouse_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    deny(monkeypatch, under(warehouse_dir), os.W_OK)
    assert ce.main(full_env(warehouse_dir, "follow")) == 1
    assert "directory not writable" in capsys.readouterr().out


def test_main_fails_on_a_landing_zone_without_complete_batches(
    landing_zones: list[Path], warehouse_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    funding = landing_zones[1]
    for batch_dir in funding.iterdir():
        (batch_dir / "_manifest.json").unlink()
    assert ce.main(full_env(warehouse_dir)) == 1
    assert "landing zone funding_rate" in capsys.readouterr().out
