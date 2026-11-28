"""Extract the API tail of the USD-M universe: the hours the archive has not published yet.

Run from repo root: python -m ingestion.extract_um_api [--nothing-new-exit-code N]
Contracts: the universe members on the newest day of the last dbt build (universe.py);
none before the first build. Funding: also the members of the last FUNDING_MEMBER_DAYS
days (the funding archive is monthly; labels of a former member need the current month).
Six series, each with its own landing zone and watermark, by the algorithms of the
whitelist's extracts: futures (last price) klines 1h, funding, open interest
and the three long/short ratios. History of a contract new to a series: the last
API_HISTORY_DAYS (the archive holds everything older; the /futures/data/ series serve
about 30 days anyway). Taker buy volume comes with the klines, so the taker ratio endpoint
is not extracted (it reads slightly below the klines, see extract_long_short).
One as-of moment for all series.

First, before the universe is read: the contract list of exchangeInfo (type, underlying,
status) as one file, um_contracts/<batch_id>/CONTRACTS.json, written only when it changed
since the newest batch. The universe leaves out TradFi contracts (stocks, metals, oil:
contractType TRADIFI_PERPETUAL, listed weekly since 2025) and index contracts by it.
"""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ingestion import extract_funding, extract_klines, futures_stats
from ingestion.batches import BatchWriter, complete_batches
from ingestion.clients.binance import BinanceFuturesClient
from ingestion.extract_cli import exit_code, parse_extract_args
from ingestion.futures_stats import PERIOD, StatsSeries
from ingestion.settings import LANDING_ZONE_DIR
from ingestion.universe import FUNDING_MEMBER_DAYS, latest_members, read_ranks, recent_members

API_HISTORY_DAYS = 35
KLINES_DIR = LANDING_ZONE_DIR / "um_api_klines_1h"
FUNDING_DIR = LANDING_ZONE_DIR / "um_api_funding_rate"
CONTRACTS_DIR = LANDING_ZONE_DIR / "um_contracts"
CONTRACTS_FILE = "CONTRACTS"  # one file per batch: every USDT contract of exchangeInfo
CONTRACT_FIELDS = (
    "symbol",
    "pair",
    "contractType",
    "underlyingType",
    "underlyingSubType",
    "status",
    "onboardDate",
    "deliveryDate",
    "quoteAsset",
)
STATS_SERIES = (
    StatsSeries(name=f"um_api_open_interest_{PERIOD}", endpoint="openInterestHist"),
    StatsSeries(name=f"um_api_top_account_ratio_{PERIOD}", endpoint="topLongShortAccountRatio"),
    StatsSeries(name=f"um_api_top_position_ratio_{PERIOD}", endpoint="topLongShortPositionRatio"),
    StatsSeries(
        name=f"um_api_global_account_ratio_{PERIOD}", endpoint="globalLongShortAccountRatio"
    ),
)


def contracts_snapshot(exchange_info: dict[str, Any]) -> list[dict[str, Any]]:
    """The USDT contracts of exchangeInfo, CONTRACT_FIELDS only, by symbol."""
    rows = [
        {field: s.get(field) for field in CONTRACT_FIELDS}
        for s in exchange_info["symbols"]
        if s.get("quoteAsset") == "USDT"
    ]
    return sorted(rows, key=lambda r: r["symbol"])


def latest_snapshot(root: Path) -> list[dict[str, Any]] | None:
    batches = complete_batches(root)
    if not batches:
        return None
    return json.loads((batches[-1] / f"{CONTRACTS_FILE}.json").read_text(encoding="utf-8"))


def extract_contracts(client: BinanceFuturesClient, now_ms: int, output_dir: Path) -> Path | None:
    """A batch with the contract list when it differs from the newest batch's."""
    snapshot = contracts_snapshot(client.exchange_info())
    if snapshot == latest_snapshot(output_dir):
        print(f"Contracts: {len(snapshot)}, unchanged")
        return None
    symbols = [r["symbol"] for r in snapshot]
    stats = {"rows": len(snapshot), "duplicates": len(symbols) - len(set(symbols))}
    writer = BatchWriter(output_dir, now_ms, "contracts")
    writer.add(CONTRACTS_FILE, snapshot, stats)
    print(f"Contracts: {len(snapshot)}, changed: new batch")
    return writer.finish()


def extract_all(
    client: BinanceFuturesClient,
    contracts: list[str],
    funding_contracts: list[str],
    now_ms: int,
) -> list[Path]:
    """Every series for the contracts (funding for funding_contracts); the batches written."""
    written: list[Path | None] = []
    print("\n== um_api_klines_1h ==")
    written.append(
        extract_klines.extract_batch(client, contracts, now_ms, KLINES_DIR, API_HISTORY_DAYS)
    )
    print("\n== um_api_funding_rate ==")
    written.append(
        extract_funding.extract_batch(
            client, funding_contracts, now_ms, FUNDING_DIR, API_HISTORY_DAYS
        )
    )
    for series in STATS_SERIES:
        print(f"\n== {series.name} ==")
        written.append(futures_stats.extract_batch(client, series, contracts, now_ms))
    return [b for b in written if b is not None]


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_extract_args("Extract the API tail of the USD-M universe.", argv)
    now_ms = int(time.time() * 1000)
    with BinanceFuturesClient() as client:
        written = [b for b in [extract_contracts(client, now_ms, CONTRACTS_DIR)] if b]
        ranks = read_ranks()
        contracts = latest_members(ranks)
        if contracts:
            funding_contracts = recent_members(ranks, FUNDING_MEMBER_DAYS)
            print(
                f"Extracting the API tail of {len(contracts)} universe contracts "
                f"(funding: {len(funding_contracts)})"
            )
            written += extract_all(client, contracts, funding_contracts, now_ms)
        else:
            print("No universe yet: no API tail to extract")
    return exit_code(written[0] if written else None, args.nothing_new_exit_code)


if __name__ == "__main__":
    sys.exit(main())
