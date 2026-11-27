"""The datasets of the landing zone and the RAW table each one is loaded into.

The landing directory is the extract's watermark (renaming it would restart the history -
see tests/test_datasets.py); the table names are the ones docs/baseline/
snowflake_baseline.sql reports, so RAW can be compared with the baseline.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ingestion import (
    extract_funding,
    extract_klines,
    extract_long_short,
    extract_open_interest,
    extract_um_api,
)
from ingestion.binance_archive import UM_FUNDING, UM_KLINES_1D, UM_KLINES_1H, UM_METRICS


@dataclass(frozen=True)
class RawDataset:
    name: str
    landing_dir: Path
    table: str  # schema-qualified RAW table; a fixed name, it goes into SQL as is
    # Grid key of a row (DuckDB SQL over payload), for duplicate counts: must match the
    # extract's check, which the manifest's `duplicates` comes from.
    key_sql: str
    # Staging reads it incrementally, batches newer than the newest it has: the loader then
    # refuses a batch older than one already loaded (it would never reach staging).
    incremental: bool = False


TIMESTAMP_KEY = "(payload->>'timestamp')::bigint"


KLINES = RawDataset(
    name="klines",
    landing_dir=extract_klines.OUTPUT_DIR,
    table="raw.binance_spot_klines_1h",
    key_sql="(payload->>0)::bigint",  # open_time
)
FUNDING_RATE = RawDataset(
    name="funding_rate",
    landing_dir=extract_funding.OUTPUT_DIR,
    table="raw.binance_futures_funding_rate",
    key_sql="(payload->>'fundingTime')::bigint // 3600000",  # the hour: fundingTime has ms jitter
)
OPEN_INTEREST = RawDataset(
    name="open_interest",
    landing_dir=extract_open_interest.OUTPUT_DIR,
    table="raw.binance_futures_open_interest_1h",
    key_sql=TIMESTAMP_KEY,
)
TOP_ACCOUNT_RATIO = RawDataset(
    name="top_account_ratio",
    landing_dir=extract_long_short.TOP_ACCOUNT_RATIO.output_dir,
    table="raw.binance_futures_top_account_ratio_1h",
    key_sql=TIMESTAMP_KEY,
)
TOP_POSITION_RATIO = RawDataset(
    name="top_position_ratio",
    landing_dir=extract_long_short.TOP_POSITION_RATIO.output_dir,
    table="raw.binance_futures_top_position_ratio_1h",
    key_sql=TIMESTAMP_KEY,
)
GLOBAL_ACCOUNT_RATIO = RawDataset(
    name="global_account_ratio",
    landing_dir=extract_long_short.GLOBAL_ACCOUNT_RATIO.output_dir,
    table="raw.binance_futures_global_account_ratio_1h",
    key_sql=TIMESTAMP_KEY,
)

# The whitelist's datasets: the ones the Snowflake baseline has, required by the pipeline.
BASELINE_DATASETS = (
    KLINES,
    FUNDING_RATE,
    OPEN_INTEREST,
    TOP_ACCOUNT_RATIO,
    TOP_POSITION_RATIO,
    GLOBAL_ACCOUNT_RATIO,
)

# USD-M universe, history from data.binance.vision (payload: the CSV row keyed by column).
ARCHIVE_KLINES_KEY = "(payload->>'open_time')::bigint"
UM_ARCHIVE_KLINES_1D = RawDataset(
    name="um_archive_klines_1d",
    landing_dir=UM_KLINES_1D.output_dir,
    table="raw.binance_um_archive_klines_1d",
    key_sql=ARCHIVE_KLINES_KEY,
    incremental=True,
)
UM_ARCHIVE_KLINES_1H = RawDataset(
    name="um_archive_klines_1h",
    landing_dir=UM_KLINES_1H.output_dir,
    table="raw.binance_um_archive_klines_1h",
    key_sql=ARCHIVE_KLINES_KEY,
    incremental=True,
)
UM_ARCHIVE_FUNDING_RATE = RawDataset(
    name="um_archive_funding_rate",
    landing_dir=UM_FUNDING.output_dir,
    table="raw.binance_um_archive_funding_rate",
    key_sql="(payload->>'calc_time')::bigint // 3600000",  # the hour: calc_time has ms jitter
    incremental=True,
)
UM_ARCHIVE_METRICS_1H = RawDataset(
    name="um_archive_metrics_1h",
    landing_dir=UM_METRICS.output_dir,
    table="raw.binance_um_archive_metrics_1h",
    key_sql="payload->>'create_time'",
    incremental=True,
)
# USD-M universe, the API tail (the last days the archive has not published yet).
UM_API_KLINES_1H = RawDataset(
    name="um_api_klines_1h",
    landing_dir=extract_um_api.KLINES_DIR,
    table="raw.binance_um_api_klines_1h",
    key_sql="(payload->>0)::bigint",
)
UM_API_FUNDING_RATE = RawDataset(
    name="um_api_funding_rate",
    landing_dir=extract_um_api.FUNDING_DIR,
    table="raw.binance_um_api_funding_rate",
    key_sql="(payload->>'fundingTime')::bigint // 3600000",
)
UM_API_STATS = tuple(
    RawDataset(
        name=series.name.removesuffix("_1h"),
        landing_dir=series.output_dir,
        table=f"raw.binance_{series.name}",
        key_sql=TIMESTAMP_KEY,
    )
    for series in extract_um_api.STATS_SERIES
)
UM_CONTRACTS = RawDataset(
    name="um_contracts",
    landing_dir=extract_um_api.CONTRACTS_DIR,
    table="raw.binance_um_contracts",
    key_sql="payload->>'symbol'",
)

# Loaded in this order.
DATASETS = (
    *BASELINE_DATASETS,
    UM_ARCHIVE_KLINES_1D,
    UM_ARCHIVE_KLINES_1H,
    UM_ARCHIVE_FUNDING_RATE,
    UM_ARCHIVE_METRICS_1H,
    UM_API_KLINES_1H,
    UM_API_FUNDING_RATE,
    *UM_API_STATS,
    UM_CONTRACTS,
)
