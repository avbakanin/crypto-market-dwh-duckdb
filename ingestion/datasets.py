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
)


@dataclass(frozen=True)
class RawDataset:
    name: str
    landing_dir: Path
    table: str  # schema-qualified RAW table; a fixed name, it goes into SQL as is
    # Grid key of a row (DuckDB SQL over payload), for duplicate counts: must match the
    # extract's check, which the manifest's `duplicates` comes from.
    key_sql: str


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
# Loaded in this order.
DATASETS = (
    KLINES,
    FUNDING_RATE,
    OPEN_INTEREST,
    TOP_ACCOUNT_RATIO,
    TOP_POSITION_RATIO,
    GLOBAL_ACCOUNT_RATIO,
)
