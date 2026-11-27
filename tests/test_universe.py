"""The universe as the extracts read it, the loader's order guard, the API tail's start."""

from __future__ import annotations

import dataclasses
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

from ingestion import extract_um_api, load_raw, universe
from ingestion.batches import MANIFEST_NAME
from ingestion.datasets import UM_ARCHIVE_KLINES_1D
from ingestion.warehouse import connect

D = date(2026, 11, 1)
DBT_PROJECT = Path(__file__).resolve().parents[1] / "dbt" / "dbt_project.yml"


def test_universe_size_matches_the_dbt_var() -> None:
    project = yaml.safe_load(DBT_PROJECT.read_text(encoding="utf-8"))
    assert project["vars"]["um_universe_size"] == universe.UNIVERSE_SIZE


def test_metrics_days_cover_the_month_before_a_member_enters() -> None:
    ranks = [
        (D, "AUSDT", 120),  # within the download rank: that day only
        (D, "BUSDT", 90),  # a member: that day and the 30 before it
        (D + timedelta(days=1), "BUSDT", 90),
    ]
    wanted = universe.metrics_days(ranks, date(2020, 9, 1), D + timedelta(days=5))
    assert wanted["AUSDT"] == {D}
    assert min(wanted["BUSDT"]) == D - timedelta(days=30)
    # the newest universe day stands for the days after it, up to the last day asked for
    assert max(wanted["BUSDT"]) == D + timedelta(days=5)
    assert len(wanted["BUSDT"]) == 30 + 1 + 5


def test_metrics_days_stay_within_the_bounds() -> None:
    ranks = [(date(2020, 9, 10), "BTCUSDT", 1)]
    wanted = universe.metrics_days(ranks, date(2020, 9, 1), date(2020, 9, 10))
    assert min(wanted["BTCUSDT"]) == date(2020, 9, 1)
    assert universe.metrics_days([], date(2020, 9, 1), date(2020, 9, 10)) == {}


def test_latest_members_and_ever_ranked() -> None:
    ranks = [
        (D, "OLDUSDT", 1),
        (D + timedelta(days=1), "BTCUSDT", 1),
        (D + timedelta(days=1), "XUSDT", 101),
    ]
    assert universe.latest_members(ranks) == ["BTCUSDT"]
    assert universe.ever_ranked(ranks) == {"OLDUSDT", "BTCUSDT", "XUSDT"}
    assert universe.latest_members([]) == []


def test_read_ranks_without_a_universe_selects_nothing(tmp_path: Path) -> None:
    assert universe.read_ranks(tmp_path / "missing.duckdb") == []
    database = tmp_path / "w.duckdb"
    with connect(database) as con:
        con.execute("create schema staging")
    assert universe.read_ranks(database) == []
    with connect(database) as con:
        con.execute(
            "create table staging.int_um__universe_daily as select * from (values "
            "(date '2026-11-01', 'BTCUSDT', 1), (date '2026-11-01', 'FARUSDT', 151)) "
            "t(universe_date, symbol, turnover_rank)"
        )
    assert universe.read_ranks(database) == [(D, "BTCUSDT", 1)]


class FakeInfo:
    def __init__(self, symbols: list[dict[str, str]]) -> None:
        self.symbols = symbols

    def __enter__(self) -> FakeInfo:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def exchange_info(self) -> dict[str, list[dict[str, str]]]:
        return {"symbols": self.symbols}


def contract(symbol: str, contract_type: str = "PERPETUAL") -> dict[str, str]:
    return {"symbol": symbol, "contractType": contract_type, "quoteAsset": "USDT", "extra": "x"}


def test_contracts_are_written_only_when_they_change(tmp_path: Path) -> None:
    root = tmp_path / "um_contracts"
    info: Any = FakeInfo([contract("XAUUSDT", "TRADIFI_PERPETUAL"), contract("BTCUSDT")])
    first = extract_um_api.extract_contracts(info, 1_795_000_000_000, root)
    assert first is not None
    rows = json.loads((first / "CONTRACTS.json").read_text(encoding="utf-8"))
    assert [r["symbol"] for r in rows] == ["BTCUSDT", "XAUUSDT"]
    assert "extra" not in rows[0] and rows[1]["contractType"] == "TRADIFI_PERPETUAL"
    assert extract_um_api.extract_contracts(info, 1_795_000_001_000, root) is None
    info.symbols.append(contract("ETHBTC") | {"quoteAsset": "BTC"})  # not a USDT contract
    assert extract_um_api.extract_contracts(info, 1_795_000_002_000, root) is None
    info.symbols.append(contract("NEWUSDT"))
    assert extract_um_api.extract_contracts(info, 1_795_000_003_000, root) is not None


def test_um_api_without_a_universe_writes_the_contracts_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(extract_um_api, "read_ranks", lambda: [])
    monkeypatch.setattr(extract_um_api, "CONTRACTS_DIR", tmp_path / "c")
    info = FakeInfo([contract("BTCUSDT")])
    monkeypatch.setattr(extract_um_api, "BinanceFuturesClient", lambda: info)
    assert extract_um_api.main(["--nothing-new-exit-code", "99"]) == 0  # the contract list
    assert extract_um_api.main(["--nothing-new-exit-code", "99"]) == 99  # unchanged: nothing


def _batch(root: Path, batch_id: str) -> None:
    batch_dir = root / batch_id
    batch_dir.mkdir(parents=True)
    row = {"open_time": "1577836800000"}
    (batch_dir / "BTCUSDT.json").write_text(json.dumps([row]), encoding="utf-8")
    manifest = {
        "batch_id": batch_id,
        "as_of_utc": "",
        "interval": "",
        "symbols": {"BTCUSDT": {"rows": 1, "duplicates": 0}},
    }
    (batch_dir / MANIFEST_NAME).write_text(json.dumps(manifest), encoding="utf-8")


def test_incremental_dataset_refuses_a_batch_older_than_one_loaded(tmp_path: Path) -> None:
    # Staging reads batches newer than its newest: a late older batch would be skipped.
    root = tmp_path / "raw"
    dataset = dataclasses.replace(UM_ARCHIVE_KLINES_1D, landing_dir=root)
    _batch(root, "20261127T000002Z")
    with connect(tmp_path / "w.duckdb") as con:
        load_raw.ensure_raw_objects(con)
        assert load_raw.load_pending(con, dataset) == (["20261127T000002Z"], [])
        _batch(root, "20261127T000001Z")
        _batch(root, "20261127T000003Z")
        loaded, problems = load_raw.load_pending(con, dataset)
        assert loaded == []
        assert problems and "older than 20261127T000002Z" in problems[0]
        # a dataset read in full takes it: the guard is for incremental ones only
        full = dataclasses.replace(dataset, incremental=False)
        assert load_raw.load_pending(con, full)[0] == ["20261127T000001Z", "20261127T000003Z"]
