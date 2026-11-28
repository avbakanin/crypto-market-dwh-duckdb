"""Tests for the dataset registry: load order and the landing-zone directories."""

from __future__ import annotations

from ingestion.datasets import DATASETS
from ingestion.settings import LANDING_ZONE_DIR


def test_datasets_are_loaded_klines_first() -> None:
    assert [d.name for d in DATASETS] == [
        "klines",
        "funding_rate",
        "open_interest",
        "top_account_ratio",
        "top_position_ratio",
        "global_account_ratio",
        "um_archive_klines_1d",
        "um_archive_klines_1h",
        "um_archive_funding_rate",
        "um_archive_metrics_1h",
        "um_api_klines_1h",
        "um_api_funding_rate",
        "um_api_open_interest",
        "um_api_top_account_ratio",
        "um_api_top_position_ratio",
        "um_api_global_account_ratio",
        "um_contracts",
        "um_liquidations",
        "um_archive_premium_index_1h",
        "spot_archive_klines_1d",
        "um_archive_book_depth_1h",
        "um_api_premium_index_1h",
    ]


def test_only_archive_datasets_are_incremental() -> None:
    # Their staging reads batches newer than its newest: the loader guards the order.
    assert [d.name for d in DATASETS if d.incremental] == [
        "um_archive_klines_1d",
        "um_archive_klines_1h",
        "um_archive_funding_rate",
        "um_archive_metrics_1h",
        "um_archive_premium_index_1h",
        "spot_archive_klines_1d",
        "um_archive_book_depth_1h",
    ]


def test_landing_zones_keep_their_directories() -> None:
    # The landing zone is the extract's watermark: a renamed directory would start the
    # history over (a year of klines, 30 days of open interest) and orphan the old batches.
    # relative_to also fails if an extract writes outside LANDING_ZONE_DIR.
    assert {d.name: d.landing_dir.relative_to(LANDING_ZONE_DIR).as_posix() for d in DATASETS} == {
        "klines": "klines_1h",
        "funding_rate": "funding_rate",
        "open_interest": "open_interest_1h",
        "top_account_ratio": "top_account_ratio_1h",
        "top_position_ratio": "top_position_ratio_1h",
        "global_account_ratio": "global_account_ratio_1h",
        "um_archive_klines_1d": "archive_um_klines_1d",
        "um_archive_klines_1h": "archive_um_klines_1h",
        "um_archive_funding_rate": "archive_um_funding_rate",
        "um_archive_metrics_1h": "archive_um_metrics_1h",
        "um_api_klines_1h": "um_api_klines_1h",
        "um_api_funding_rate": "um_api_funding_rate",
        "um_api_open_interest": "um_api_open_interest_1h",
        "um_api_top_account_ratio": "um_api_top_account_ratio_1h",
        "um_api_top_position_ratio": "um_api_top_position_ratio_1h",
        "um_api_global_account_ratio": "um_api_global_account_ratio_1h",
        "um_contracts": "um_contracts",
        "um_liquidations": "um_liquidations",
        "um_archive_premium_index_1h": "archive_um_premium_index_1h",
        "spot_archive_klines_1d": "archive_spot_klines_1d",
        "um_archive_book_depth_1h": "archive_um_book_depth_1h",
        "um_api_premium_index_1h": "um_api_premium_index_1h",
    }
