"""Tests for where the landing zone is: LANDING_ZONE_DIR, relative to the repo root."""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion import settings
from tests.conftest import SANDBOX_LANDING_ZONE


@pytest.mark.parametrize("env", [{}, {"LANDING_ZONE_DIR": ""}], ids=["unset", "empty"])
def test_default_landing_zone_is_data_raw_in_the_repo(env: dict[str, str]) -> None:
    assert settings.landing_zone_dir(env) == settings.PROJECT_ROOT / "data" / "raw"


def test_relative_landing_zone_is_resolved_against_the_repo_root() -> None:
    env = {"LANDING_ZONE_DIR": "./data/raw"}
    assert settings.landing_zone_dir(env) == settings.PROJECT_ROOT / "data" / "raw"


def test_absolute_landing_zone_is_kept(tmp_path: Path) -> None:
    assert settings.landing_zone_dir({"LANDING_ZONE_DIR": str(tmp_path)}) == tmp_path


def test_tests_never_see_the_real_landing_zone() -> None:
    # tests/conftest.py sets the variable before ingestion is imported.
    assert settings.LANDING_ZONE_DIR == SANDBOX_LANDING_ZONE


def test_default_warehouse_is_in_the_repo() -> None:
    assert settings.warehouse_path({}) == settings.PROJECT_ROOT / "warehouse" / "crypto_dwh.duckdb"


def test_relative_warehouse_is_resolved_against_the_repo_root() -> None:
    env = {"DUCKDB_PATH": "warehouse/other.duckdb"}
    assert settings.warehouse_path(env) == settings.PROJECT_ROOT / "warehouse" / "other.duckdb"


def test_warehouse_path_is_read_on_each_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "a.duckdb"))
    assert settings.warehouse_path() == tmp_path / "a.duckdb"
