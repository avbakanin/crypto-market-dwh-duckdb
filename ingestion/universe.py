"""The point-in-time universe of USD-M perpetuals, as the extracts need it.

dbt builds it (int_um__universe_daily: a turnover rank per eligible contract and day); the
extracts read the last build to decide what to download:
- the API tail: the members (rank <= UNIVERSE_SIZE) on the newest day; funding for the
  members of the last FUNDING_MEMBER_DAYS days (the archive has funding by month only, so
  a contract that left the top keeps needing the current month for its labels);
- archive klines and funding: every contract ever ranked within DOWNLOAD_RANK;
- archive metrics (the heavy part, one file a day): day d of a contract when it ranks
  within DOWNLOAD_RANK that day, or becomes a member within LOOKAHEAD_DAYS after it, so a
  newcomer has a month of history when it enters. The future decides only what to download,
  never a feature: features read days up to their own date.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import duckdb

from ingestion.warehouse import connect

UNIVERSE_SIZE = 100  # must equal the dbt var um_universe_size (a test checks it)
DOWNLOAD_RANK = 150
LOOKAHEAD_DAYS = 30
# The month the funding archive has not published yet (up to ~32 days early in a month)
# plus the longest label horizon (14 days), with a margin.
FUNDING_MEMBER_DAYS = 50
UNIVERSE_TABLE = "staging.int_um__universe_daily"

Rank = tuple[date, str, int]  # (universe_date, symbol, turnover_rank)


def read_ranks(path: Path | None = None) -> list[Rank]:
    """Ranks within DOWNLOAD_RANK from the warehouse; empty when it has no universe yet."""
    try:
        with connect(path, read_only=True) as con:
            return [
                (d, s, r)
                for d, s, r in con.execute(
                    f"select universe_date, symbol, turnover_rank from {UNIVERSE_TABLE} "
                    "where turnover_rank <= ? order by universe_date, turnover_rank",
                    [DOWNLOAD_RANK],
                ).fetchall()
            ]
    except (duckdb.CatalogException, duckdb.IOException, FileNotFoundError) as exc:
        print(f"No universe in the warehouse yet ({type(exc).__name__}): nothing selected")
        return []


def latest_members(ranks: list[Rank]) -> list[str]:
    """Members on the newest universe day, by rank."""
    if not ranks:
        return []
    newest = max(d for d, _, _ in ranks)
    return [s for d, s, r in ranks if d == newest and r <= UNIVERSE_SIZE]


def recent_members(ranks: list[Rank], days: int) -> list[str]:
    """Members on any of the newest `days` universe days: the newest day's members by rank,
    then the others by name."""
    if not ranks:
        return []
    newest = max(d for d, _, _ in ranks)
    latest = latest_members(ranks)
    first = newest - timedelta(days=days - 1)
    others = {s for d, s, r in ranks if d >= first and r <= UNIVERSE_SIZE} - set(latest)
    return latest + sorted(others)


def ever_ranked(ranks: list[Rank]) -> set[str]:
    """Contracts ranked within DOWNLOAD_RANK on any day."""
    return {s for _, s, _ in ranks}


def metrics_days(ranks: list[Rank], first_day: date, last_day: date) -> dict[str, set[date]]:
    """Days of metrics to keep per contract within [first_day, last_day] (module docstring).
    Days after the newest universe day take its ranks: the universe lags the data by days."""
    wanted: dict[str, set[date]] = defaultdict(set)
    if not ranks:
        return {}
    newest = max(d for d, _, _ in ranks)
    for day, symbol, rank in ranks:
        span = [day]
        if day == newest:
            span = [day + timedelta(days=i) for i in range((last_day - day).days + 1)] or [day]
        if rank <= UNIVERSE_SIZE:
            span += [day - timedelta(days=i) for i in range(1, LOOKAHEAD_DAYS + 1)]
        wanted[symbol].update(d for d in span if first_day <= d <= last_day)
    return {s: days for s, days in wanted.items() if days}
