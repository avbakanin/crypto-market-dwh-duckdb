"""Thin Binance REST clients: shared Session, timeouts, retries on transient errors.

BinanceClient serves spot market data (api.binance.com), BinanceFuturesClient USD-M futures
market data (fapi.binance.com). Both share the transport in BinanceRestClient; neither
inherits the other's endpoints, which exist only on its own host.
"""

from __future__ import annotations

import time
from types import TracebackType
from typing import Any, Self

import requests

SPOT_BASE_URL = "https://api.binance.com"
FUTURES_BASE_URL = "https://fapi.binance.com"
DEFAULT_TIMEOUT_S = 10
DEFAULT_MAX_RETRIES = 5
BACKOFF_BASE_S = 1.0

# Transient: worth retrying. 418 (IP ban) is deliberately NOT here.
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# /futures/data/ statistics the extracts use; same parameters, same ~30-day window.
FUTURES_DATA_ENDPOINTS = frozenset(
    {
        "openInterestHist",
        "topLongShortAccountRatio",
        "topLongShortPositionRatio",
        "globalLongShortAccountRatio",
    }
)


class BinanceAPIError(RuntimeError):
    """Non-retryable (or retries exhausted) HTTP error with Binance response body."""

    def __init__(self, status_code: int, path: str, body: str) -> None:
        super().__init__(f"HTTP {status_code} on {path}: {body}")
        self.status_code = status_code
        self.path = path
        self.body = body


class BinanceRestClient:
    """Transport shared by the spot and futures clients: Session, GET, retries."""

    DEFAULT_BASE_URL = SPOT_BASE_URL

    def __init__(
        self,
        base_url: str | None = None,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        self.base_url = base_url or self.DEFAULT_BASE_URL
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.session = requests.Session()

    # --- context manager: closes the Session ---
    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self.session.close()

    # --- core ---
    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET with retries on 429/5xx and network errors; raise BinanceAPIError otherwise."""
        url = f"{self.base_url}{path}"

        for attempt in range(1, self.max_retries + 1):
            is_last = attempt == self.max_retries
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_s)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if is_last:
                    raise
                wait = self._backoff(attempt)
                print(
                    f"  {type(exc).__name__} on {path}, "
                    f"retry {attempt}/{self.max_retries} in {wait:.0f}s"
                )
                time.sleep(wait)
                continue

            if response.ok:
                return response.json()

            if response.status_code in RETRY_STATUS_CODES and not is_last:
                wait = self._retry_after(response) or self._backoff(attempt)
                print(
                    f"  HTTP {response.status_code} on {path}, "
                    f"retry {attempt}/{self.max_retries} in {wait:.0f}s"
                )
                time.sleep(wait)
                continue

            raise BinanceAPIError(response.status_code, path, response.text)

        raise AssertionError("unreachable")  # keeps type checker happy

    @staticmethod
    def _backoff(attempt: int) -> float:
        return BACKOFF_BASE_S * 2 ** (attempt - 1)  # 1, 2, 4, 8 ...

    @staticmethod
    def _retry_after(response: requests.Response) -> float | None:
        header = response.headers.get("Retry-After")
        try:
            return float(header) if header else None
        except ValueError:
            return None


class BinanceClient(BinanceRestClient):
    """Spot market data endpoints."""

    DEFAULT_BASE_URL = SPOT_BASE_URL

    def exchange_info(self) -> dict[str, Any]:
        return self.get("/api/v3/exchangeInfo")

    def ticker_24h(self) -> list[dict[str, Any]]:
        return self.get("/api/v3/ticker/24hr")

    def klines(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[list[Any]]:
        """Raw klines. start_time/end_time are Unix ms UTC (DQ note #4)."""
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return self.get("/api/v3/klines", params)


class BinanceFuturesClient(BinanceRestClient):
    """USD-M futures market data endpoints (perpetual contracts: funding rate, open interest,
    long/short ratios)."""

    DEFAULT_BASE_URL = FUTURES_BASE_URL

    def exchange_info(self) -> dict[str, Any]:
        return self.get("/fapi/v1/exchangeInfo")

    def klines(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[list[Any]]:
        """Futures (last price) klines, the spot array layout; limit up to 1500."""
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return self.get("/fapi/v1/klines", params)

    def funding_info(self) -> list[dict[str, Any]]:
        """Contracts whose funding cap, floor or interval was adjusted; others use the defaults."""
        return self.get("/fapi/v1/fundingInfo")

    def premium_index(self) -> list[dict[str, Any]]:
        """Mark price, last funding rate and interestRate of every contract, as of now.

        interestRate is the contract's base rate per 8 hours: 0.0001 for most, 0 for some
        (BNBUSDT). While the premium stays in the clamp band a settlement pays exactly
        interestRate * interval_hours / 8, the most frequent funding rate of a contract.
        """
        return self.get("/fapi/v1/premiumIndex")

    def funding_rate(
        self,
        symbol: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Funding rate history, ascending: the first `limit` records with
        start_time <= fundingTime <= end_time (both inclusive, Unix ms UTC)."""
        if start_time is not None and start_time <= 0:
            # Live: startTime=0 is taken as "not sent" and returns the most recent records.
            raise ValueError(f"start_time must be positive, got {start_time}")
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return self.get("/fapi/v1/fundingRate", params)

    def futures_data(
        self,
        endpoint: str,
        symbol: str,
        period: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Statistics of /futures/data/<endpoint>, ascending: the LAST `limit` points with
        start_time <= timestamp <= end_time (unlike klines and funding, which return the first).

        Only about the latest month is served: an older start_time or end_time is HTTP 400.
        """
        if endpoint not in FUTURES_DATA_ENDPOINTS:
            raise ValueError(f"Unknown /futures/data/ endpoint {endpoint!r}")
        params: dict[str, Any] = {"symbol": symbol, "period": period, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return self.get(f"/futures/data/{endpoint}", params)
