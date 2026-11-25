"""Tests for the dataset registry: load order and the landing-zone directories."""

from __future__ import annotations

from ingestion.datasets import DATASETS
from ingestion.extract_klines import PROJECT_ROOT


def test_datasets_are_loaded_klines_first() -> None:
    assert [d.name for d in DATASETS] == [
        "klines",
        "funding_rate",
        "open_interest",
        "top_account_ratio",
        "top_position_ratio",
        "global_account_ratio",
    ]


def test_landing_zones_keep_their_directories() -> None:
    # The landing zone is the extract's watermark: a renamed directory would start the
    # history over (a year of klines, 30 days of open interest) and orphan the old batches.
    root = PROJECT_ROOT / "data" / "raw"
    assert {d.name: d.landing_dir.relative_to(root).as_posix() for d in DATASETS} == {
        "klines": "klines_1h",
        "funding_rate": "funding_rate",
        "open_interest": "open_interest_1h",
        "top_account_ratio": "top_account_ratio_1h",
        "top_position_ratio": "top_position_ratio_1h",
        "global_account_ratio": "global_account_ratio_1h",
    }
