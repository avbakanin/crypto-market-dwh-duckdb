"""From scored candidates to trades: at most one a day, one open position per symbol."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from research.stats import as_date


def select_trades(candidates: pd.DataFrame, horizon: int, per_day: int = 1) -> pd.DataFrame:
    """Candidates: decision_date, symbol, side ('long' / 'short'), score (higher = better;
    only rows with a score > 0 are eligible). Each day, the best eligible candidates up to
    `per_day` whose symbol has no open position (entered within the last `horizon` days,
    either side) become trades. Days in order, so a trade blocks only later days."""
    eligible = candidates.loc[candidates["score"] > 0].sort_values(
        by=["decision_date", "score", "symbol"], ascending=[True, False, True]
    )
    open_until: dict[str, date] = {}
    chosen: list[int] = []
    for key, group in eligible.groupby("decision_date", sort=True):
        day = as_date(key)
        taken = 0
        for index, symbol in zip(group.index, group["symbol"], strict=True):
            if taken == per_day:
                break
            if symbol in open_until and open_until[symbol] > day:
                continue
            chosen.append(index)
            open_until[symbol] = day + timedelta(days=horizon)
            taken += 1
    return eligible.loc[chosen].reset_index(drop=True)
