"""One decision day: the members' features of D, the frozen model's scores, one pick.

The pick follows research.selection.select_trades day by day: the best candidate with a
score > 0 whose symbol has no open position of the version (a trade blocks its symbol for
the horizon, whatever its outcome, so the choice depends on the log alone). hedged mode:
BTCUSDT is never picked; the hedge is beta_btc_30d of the pick (D-1, clipped to [0, 3],
1 when missing) times its notional in the opposite BTCUSDT trade.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

from research import config, data, strategies
from signals import journal, registry
from signals.models import FrozenModel

EXTRA = ("history_days", "turnover_rank", "beta_btc_30d")
TOP_CANDIDATES = 5


class DataNotReady(RuntimeError):
    """The features of the decision day are incomplete: no decision yet."""


def check_day(features_day: pd.DataFrame, day: date, universe_size: int) -> None:
    """D's features: every member, every hour of D-1 behind them, BTCUSDT among them."""
    problems = []
    if len(features_day) != universe_size:
        problems.append(f"{len(features_day)} members, expected {universe_size}")
    if len(features_day):
        if bool((features_day["hours_feature_day"].to_numpy() != 24).any()):
            problems.append("D-1 is not complete (hours_feature_day < 24)")
        feature_days = set(pd.to_datetime(features_day["feature_date"]).dt.date)
        if feature_days != {day - timedelta(days=1)}:
            problems.append(f"feature_date {sorted(feature_days)}, expected {day - timedelta(1)}")
    if registry.HEDGE_SYMBOL not in set(features_day["symbol"]):
        problems.append(f"{registry.HEDGE_SYMBOL} is not a member")
    if problems:
        raise DataNotReady(f"features of {day}: " + "; ".join(problems))


def apply_rules(scored: pd.DataFrame, panel: pd.DataFrame, rules: registry.RiskRules):
    """Drop the shorts the risk rules forbid (young or illiquid contracts)."""
    info = panel.set_index("symbol")
    short = scored["side"].to_numpy() == "short"
    banned = np.zeros(len(scored), dtype=bool)
    if rules.short_min_history_days is not None:
        history = info["history_days"].reindex(scored["symbol"]).to_numpy(dtype=float)
        banned |= short & ~(history >= rules.short_min_history_days)
    if rules.short_max_turnover_rank is not None:
        rank = info["turnover_rank"].reindex(scored["symbol"]).to_numpy(dtype=float)
        banned |= short & ~(rank <= rules.short_max_turnover_rank)
    return scored.loc[~banned]


def open_trades(history: list[dict[str, Any]], version: str, day: date, h: int) -> list[dict]:
    """The version's trades still open on `day` (decided within the last h days)."""
    return [
        row
        for row in history
        if row["version"] == version
        and row["status"] == journal.TRADE
        and day - timedelta(days=h) < row["decision_date"] < day
    ]


def decide(
    version: registry.ForwardVersion,
    model: FrozenModel,
    features_day: pd.DataFrame,
    history: list[dict[str, Any]],
    day: date,
) -> dict[str, Any]:
    h = version.horizon
    panel = data.feature_panel(features_day, extra=EXTRA)
    scored = model.score(panel)
    scored = strategies.apply_mode(scored, panel, version.research.mode)
    scored = apply_rules(scored, panel, version.rules)
    ranked = scored.sort_values(["score", "symbol"], ascending=[False, True])
    base: dict[str, Any] = {
        "version": version.name,
        "horizon_days": h,
        "model_sha256": model.sha256,
        "model_block": model.block.isoformat(),
        "top_candidates": [
            {"symbol": s, "side": d, "score": round(float(v), 6)}
            for s, d, v in ranked[["symbol", "side", "score"]]
            .head(TOP_CANDIDATES)
            .itertuples(index=False)
        ],
    }
    still_open = open_trades(history, version.name, day, h)
    if version.rules.max_open is not None and len(still_open) >= version.rules.max_open:
        return {**base, "status": journal.NO_TRADE, "reason": "max_open"}
    eligible = ranked.loc[ranked["score"] > 0]
    if eligible.empty:
        return {**base, "status": journal.NO_TRADE, "reason": "no_positive_score"}
    blocked = {row["symbol"] for row in still_open}
    free = eligible.loc[~eligible["symbol"].isin(blocked)]
    if free.empty:
        return {**base, "status": journal.NO_TRADE, "reason": "symbols_open"}
    pick = free.iloc[0]
    trade = {
        **base,
        "status": journal.TRADE,
        "symbol": str(pick["symbol"]),
        "side": str(pick["side"]),
        "score": round(float(pick["score"]), 6),
        "exit_time": (journal.entry_time(day) + timedelta(days=h)).isoformat(),
        "stop_loss": version.rules.stop_loss,
        "open_positions_before": len(still_open),
    }
    if version.research.mode == "hedged":
        beta = panel.set_index("symbol")["beta_btc_30d"].astype(float).get(trade["symbol"])
        ratio = (
            1.0
            if beta is None or not np.isfinite(beta)
            else float(np.clip(beta, *config.BETA_CLIP))
        )
        trade |= {
            "hedge_symbol": registry.HEDGE_SYMBOL,
            "hedge_side": "short" if trade["side"] == "long" else "long",
            "hedge_ratio": round(ratio, 6),
        }
    return trade
