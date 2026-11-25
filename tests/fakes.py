"""Test doubles: in-memory stand-ins for the Binance spot and futures clients."""

from __future__ import annotations

from typing import Any

from ingestion.clients.binance import BinanceAPIError
from ingestion.extract_klines import Kline


class FakeKlinesSource:
    """Serves klines from memory and records the start_time of every request.

    Assumed Binance semantics (consistent with DQ note #13): the API returns candles
    with open_time >= start_time, sorted ascending, at most `limit` of them.
    """

    def __init__(self, klines: list[Kline]) -> None:
        self._klines = sorted(klines, key=lambda k: k[0])
        self.calls: list[int | None] = []

    def klines(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[Kline]:
        self.calls.append(start_time)
        rows = [k for k in self._klines if start_time is None or k[0] >= start_time]
        if end_time is not None:
            rows = [k for k in rows if k[0] <= end_time]
        return rows[:limit]


class FakeBinance:
    """Serves exchangeInfo, 24h tickers and daily klines from memory for select_symbols tests.

    Assumed Binance klines semantics (verified on the live API, DQ notes #13 and #14):
    with start_time -> the first `limit` candles with open_time >= start_time;
    without start_time -> the most recent `limit` candles.
    """

    def __init__(self) -> None:
        self._symbols: list[dict[str, Any]] = []
        self._tickers: list[dict[str, Any]] = []
        self._klines: dict[str, list[Kline]] = {}
        self.kline_calls: list[str] = []  # symbol of every klines request

    def add_symbol(
        self, info: dict[str, Any], quote_volume_24h: float, daily_klines: list[Kline]
    ) -> None:
        symbol = info["symbol"]
        self._symbols.append(info)
        self._tickers.append({"symbol": symbol, "quoteVolume": f"{quote_volume_24h:.8f}"})
        self._klines[symbol] = sorted(daily_klines, key=lambda k: k[0])

    def exchange_info(self) -> dict[str, Any]:
        return {"symbols": self._symbols}

    def ticker_24h(self) -> list[dict[str, Any]]:
        return self._tickers

    def klines(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[Kline]:
        if interval != "1d":
            raise ValueError(f"FakeBinance serves only daily klines, got {interval!r}")
        self.kline_calls.append(symbol)
        rows = self._klines[symbol]  # unknown symbol -> KeyError, like Binance's 400
        if start_time is not None:
            rows = [k for k in rows if k[0] >= start_time]
        if end_time is not None:
            rows = [k for k in rows if k[0] <= end_time]
        return rows[:limit] if start_time is not None else rows[-limit:]


class FakeFutures:
    """Serves the USD-M futures endpoints from memory and records every request.

    Live semantics (verified, Week 2 Step 2): funding_rate with start_time -> the first `limit`
    records with fundingTime >= start_time (inclusive), ascending; start_time <= 0 is refused,
    as by the real client; without start_time -> the most recent `limit`. funding_info lists
    the contracts given an interval. futures_data (open interest = "openInterestHist", any
    other /futures/data/ endpoint given in `stats`) returns the LAST `limit` points with
    start_time <= timestamp <= end_time; a start_time or end_time before `served_from` is
    HTTP 400 -1130, as live for times older than about a month. An unknown contract or
    endpoint raises KeyError.
    """

    def __init__(
        self,
        funding: dict[str, list[dict[str, Any]]] | None = None,
        intervals: dict[str, int] | None = None,
        open_interest: dict[str, list[dict[str, Any]]] | None = None,
        served_from: int | None = None,
        stats: dict[str, dict[str, list[dict[str, Any]]]] | None = None,
    ) -> None:
        self._funding = {
            s: sorted(rows, key=lambda r: r["fundingTime"]) for s, rows in (funding or {}).items()
        }
        self._intervals = intervals or {}
        by_endpoint = {"openInterestHist": open_interest or {}, **(stats or {})}
        self._stats = {
            endpoint: {s: sorted(rows, key=lambda r: r["timestamp"]) for s, rows in series.items()}
            for endpoint, series in by_endpoint.items()
        }
        self._served_from = served_from
        self.info_calls = 0
        self.funding_calls: list[tuple[str, int | None]] = []  # (symbol, start_time)
        # (endpoint, symbol, start, end) of every futures_data request
        self.stats_calls: list[tuple[str, str, int | None, int | None]] = []

    def __enter__(self) -> FakeFutures:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def funding_info(self) -> list[dict[str, Any]]:
        self.info_calls += 1
        return [
            {
                "symbol": symbol,
                "adjustedFundingRateCap": "0.02000000",
                "adjustedFundingRateFloor": "-0.02000000",
                "fundingIntervalHours": hours,
                "disclaimer": False,
                "updateTime": 0,
            }
            for symbol, hours in self._intervals.items()
        ]

    def funding_rate(
        self,
        symbol: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if start_time is not None and start_time <= 0:
            raise ValueError(f"start_time must be positive, got {start_time}")
        self.funding_calls.append((symbol, start_time))
        rows = self._funding[symbol]
        if end_time is not None:
            rows = [r for r in rows if r["fundingTime"] <= end_time]
        if start_time is None:
            return rows[-limit:]
        return [r for r in rows if r["fundingTime"] >= start_time][:limit]

    def futures_data(
        self,
        endpoint: str,
        symbol: str,
        period: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        if period != "1h":
            raise ValueError(f"FakeFutures serves 1h statistics only, got {period!r}")
        self.stats_calls.append((endpoint, symbol, start_time, end_time))
        for name, value in (("startTime", start_time), ("endTime", end_time)):
            if value is not None and self._served_from is not None and value < self._served_from:
                body = '{"msg":"parameter ' + repr(name) + ' is invalid.","code":-1130}'
                raise BinanceAPIError(400, f"/futures/data/{endpoint}", body)
        rows = self._stats[endpoint][symbol]
        if start_time is not None:
            rows = [r for r in rows if r["timestamp"] >= start_time]
        if end_time is not None:
            rows = [r for r in rows if r["timestamp"] <= end_time]
        return rows[-limit:]
