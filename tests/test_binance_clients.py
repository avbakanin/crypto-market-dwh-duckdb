"""Request building of the Binance clients: host, path and query parameters, no network."""

from __future__ import annotations

from typing import Any

import pytest

from ingestion.clients.binance import (
    FUTURES_BASE_URL,
    SPOT_BASE_URL,
    BinanceClient,
    BinanceFuturesClient,
)

START_MS = 1_735_689_600_000  # 2025-01-01 00:00:00 UTC


class RecordingFutures(BinanceFuturesClient):
    """BinanceFuturesClient whose get() records the request instead of sending it."""

    def __init__(self) -> None:
        super().__init__()
        self.requests: list[tuple[str, dict[str, Any] | None]] = []

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        self.requests.append((path, params))
        return []


@pytest.fixture
def client() -> RecordingFutures:
    return RecordingFutures()


def test_each_client_talks_to_its_own_host() -> None:
    assert BinanceClient().base_url == SPOT_BASE_URL
    assert BinanceFuturesClient().base_url == FUTURES_BASE_URL


def test_futures_client_does_not_inherit_spot_endpoints() -> None:
    # A spot path sent to the futures host is a 404 at run time, not a type error.
    assert not hasattr(BinanceFuturesClient, "ticker_24h")
    assert BinanceFuturesClient.klines is not BinanceClient.klines


def test_futures_klines_request(client: RecordingFutures) -> None:
    client.klines("BTCUSDT", "1h", start_time=START_MS, limit=1000)
    client.klines("BTCUSDT", "1h", end_time=START_MS)
    assert client.requests == [
        (
            "/fapi/v1/klines",
            {"symbol": "BTCUSDT", "interval": "1h", "limit": 1000, "startTime": START_MS},
        ),
        (
            "/fapi/v1/klines",
            {"symbol": "BTCUSDT", "interval": "1h", "limit": 1000, "endTime": START_MS},
        ),
    ]


def test_premium_index_klines_serve_as_klines(client: RecordingFutures) -> None:
    # The API tail extracts them with the klines algorithm through this adapter.
    from ingestion.extract_um_api import PremiumIndexKlines

    PremiumIndexKlines(client).klines("BTCUSDT", "1h", start_time=START_MS, limit=1500)
    assert client.requests == [
        (
            "/fapi/v1/premiumIndexKlines",
            {"symbol": "BTCUSDT", "interval": "1h", "limit": 1500, "startTime": START_MS},
        )
    ]


def test_funding_rate_request(client: RecordingFutures) -> None:
    client.funding_rate("BTCUSDT", start_time=START_MS, end_time=START_MS + 1, limit=1000)
    assert client.requests == [
        (
            "/fapi/v1/fundingRate",
            {"symbol": "BTCUSDT", "limit": 1000, "startTime": START_MS, "endTime": START_MS + 1},
        )
    ]


def test_funding_rate_leaves_out_times_not_given(client: RecordingFutures) -> None:
    client.funding_rate("BTCUSDT")
    assert client.requests == [("/fapi/v1/fundingRate", {"symbol": "BTCUSDT", "limit": 1000})]


@pytest.mark.parametrize("start_time", [0, -1])
def test_funding_rate_rejects_non_positive_start_before_any_request(
    client: RecordingFutures, start_time: int
) -> None:
    # Live API: startTime=0 means "not sent" and returns the newest records, not the oldest.
    with pytest.raises(ValueError, match="positive"):
        client.funding_rate("BTCUSDT", start_time=start_time)
    assert client.requests == []


@pytest.mark.parametrize(
    "endpoint",
    [
        "openInterestHist",
        "topLongShortAccountRatio",
        "topLongShortPositionRatio",
        "globalLongShortAccountRatio",
    ],
)
def test_futures_data_request(client: RecordingFutures, endpoint: str) -> None:
    client.futures_data(endpoint, "1000PEPEUSDT", "1h", start_time=START_MS, end_time=START_MS + 1)
    assert client.requests == [
        (
            f"/futures/data/{endpoint}",
            {
                "symbol": "1000PEPEUSDT",
                "period": "1h",
                "limit": 500,
                "startTime": START_MS,
                "endTime": START_MS + 1,
            },
        )
    ]


def test_futures_data_rejects_an_unknown_endpoint_before_any_request(
    client: RecordingFutures,
) -> None:
    # A typo would be a 404 only at run time, after the retries.
    with pytest.raises(ValueError, match="openInterestHistory"):
        client.futures_data("openInterestHistory", "BTCUSDT", "1h")
    assert client.requests == []


def test_futures_reference_endpoints(client: RecordingFutures) -> None:
    client.exchange_info()
    client.funding_info()
    client.premium_index()
    # premiumIndex without a symbol: every contract in one request (weight 10).
    assert client.requests == [
        ("/fapi/v1/exchangeInfo", None),
        ("/fapi/v1/fundingInfo", None),
        ("/fapi/v1/premiumIndex", None),
    ]
