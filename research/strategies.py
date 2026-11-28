"""Signal families: fit on a train fold, score the test fold's member-days.

Each returns candidates (decision_date, symbol, side, score); research.selection turns
them into trades. Scores are comparable within a family only. Fitted state comes from the
train fold alone.
- btc_trend: BTCUSDT long while BTC closes above its 50-day SMA, short below (no fit).
- composite: the day's ranks weighted by their train rank IC against the h-day gross
  return (features with |Newey-West t| > COMPOSITE_MIN_T); the BTC trend picks the side:
  long the best score above the SMA, short the worst below.
- ridge: linear regression of the clipped net return of each side on the ranks and the
  standardized market features; a candidate per side with the predicted net as score.
- lgbm: gradient boosting of the same, missing values kept.
"""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import pandas as pd

from research import config

RANKS = [f"rank_{c}" for c in config.SYMBOL_FEATURES]


def candidates(family: str, train: pd.DataFrame, test: pd.DataFrame, h: int) -> pd.DataFrame:
    if family == "btc_trend":
        return btc_trend(test)
    if family == "composite":
        return composite(train, test, h)
    if family == "ridge":
        return regression(train, test, h, fit_ridge)
    if family == "lgbm":
        return regression(train, test, h, fit_lgbm)
    raise ValueError(f"unknown family {family}")


def apply_mode(scored: pd.DataFrame, test: pd.DataFrame, mode: str) -> pd.DataFrame:
    """hedged: BTCUSDT is the hedge, never a pick; regime: the side must follow the BTC
    trend of the day (long above the 50-day SMA, short below)."""
    if mode == "plain":
        return scored
    if mode == "hedged":
        return scored.loc[scored["symbol"] != "BTCUSDT"]
    if mode == "regime":
        trend = cast(pd.Series, test.groupby("decision_date")["btc_close_to_sma50"].first())
        lookup = dict(zip(trend.index, trend.to_numpy(dtype=float), strict=True))
        day_trend = np.array([lookup.get(d, np.nan) for d in scored["decision_date"]])
        side = scored["side"].to_numpy()
        keep = ((side == "long") & (day_trend > 0)) | ((side == "short") & (day_trend < 0))
        return scored.loc[keep]
    raise ValueError(f"unknown mode {mode}")


def btc_trend(test: pd.DataFrame) -> pd.DataFrame:
    btc = test.loc[(test["symbol"] == "BTCUSDT") & test["btc_close_to_sma50"].notna()]
    side = np.where(btc["btc_close_to_sma50"] > 0, "long", "short")
    return pd.DataFrame(
        {"decision_date": btc["decision_date"], "symbol": btc["symbol"], "side": side, "score": 1.0}
    )


def newey_west_t(x: np.ndarray, lag: int) -> float:
    x = x[~np.isnan(x)]
    n = len(x)
    if n < 2 * lag + 10:
        return 0.0
    e = x - x.mean()
    variance = (e @ e) / n
    for k in range(1, lag + 1):
        variance += 2 * (1 - k / (lag + 1)) * (e[k:] @ e[:-k]) / n
    return float(x.mean() / np.sqrt(variance / n)) if variance > 0 else 0.0


def daily_rank_ic(frame: pd.DataFrame, rank_column: str, target: str) -> pd.Series:
    """Per day, the correlation of a centred rank with the target's centred rank."""
    x = frame[["decision_date", rank_column, target]].dropna()
    sizes = cast(pd.Series, x.groupby("decision_date")[rank_column].transform("size"))
    x = x.loc[sizes >= 20]
    if x.empty:
        return pd.Series(dtype=float)
    by_day = x.groupby("decision_date")
    xd = x[rank_column] - cast(pd.Series, by_day[rank_column].transform("mean"))
    yr = cast(pd.Series, by_day[target].rank(pct=True))
    yd = yr - cast(pd.Series, yr.groupby(x["decision_date"]).transform("mean"))
    codes, days = pd.factorize(x["decision_date"], sort=True)
    xv, yv = xd.to_numpy(dtype=float), yd.to_numpy(dtype=float)
    xy = np.bincount(codes, weights=xv * yv)
    xx = np.bincount(codes, weights=xv * xv)
    yy = np.bincount(codes, weights=yv * yv)
    with np.errstate(invalid="ignore", divide="ignore"):
        ic = np.where(xx * yy > 0, xy / np.sqrt(xx * yy), np.nan)
    return pd.Series(ic, index=days)


def composite_weights(train: pd.DataFrame, h: int) -> dict[str, float]:
    weights = {}
    for column in RANKS:
        ic = daily_rank_ic(train, column, f"g_{h}")
        if len(ic) and abs(newey_west_t(ic.to_numpy(dtype=float), h)) > config.COMPOSITE_MIN_T:
            weights[column] = float(ic.mean())
    return weights


def composite(train: pd.DataFrame, test: pd.DataFrame, h: int) -> pd.DataFrame:
    weights = composite_weights(train, h)
    if not weights:
        return pd.DataFrame(columns=["decision_date", "symbol", "side", "score"])
    total = sum(abs(w) for w in weights.values())
    raw = sum(test[c].fillna(0.0) * (w / total) for c, w in weights.items())
    up = test["btc_close_to_sma50"] > 0
    down = test["btc_close_to_sma50"] < 0
    long = pd.DataFrame(
        {"decision_date": test["decision_date"], "symbol": test["symbol"], "side": "long",
         "score": raw}
    ).loc[up]  # fmt: skip
    short = pd.DataFrame(
        {"decision_date": test["decision_date"], "symbol": test["symbol"], "side": "short",
         "score": -raw}
    ).loc[down]  # fmt: skip
    return pd.concat([long, short], ignore_index=True)


def design(frame: pd.DataFrame, stats: pd.DataFrame | None, keep_missing: bool):
    """Ranks and standardized market features; stats (mean, std) come from the train fold."""
    market = frame[config.MARKET_FEATURES]
    if stats is None:
        std = cast(pd.Series, market.std())
        stats = pd.DataFrame({"mean": market.mean(), "std": std.replace(0, 1.0)})
    scaled = (market - stats["mean"]) / stats["std"]
    x = cast(pd.DataFrame, pd.concat([frame[RANKS], scaled], axis=1))
    return (x if keep_missing else x.fillna(0.0)), stats


def clipped(y: pd.Series) -> pd.Series:
    low, high = y.quantile([config.WINSOR, 1 - config.WINSOR])
    return y.clip(low, high)


def fit_ridge(x: pd.DataFrame, y: pd.Series):
    xv = x.to_numpy(dtype=float)
    yv = y.to_numpy(dtype=float)
    x_mean = xv.mean(axis=0)
    xc = xv - x_mean
    y_mean = yv.mean()
    gram = xc.T @ xc + config.RIDGE_ALPHA * np.eye(xc.shape[1])
    beta = np.linalg.solve(gram, xc.T @ (yv - y_mean))
    intercept = y_mean - x_mean @ beta
    return lambda new: new.to_numpy(dtype=float) @ beta + intercept


def fit_lgbm(x: pd.DataFrame, y: pd.Series):
    import lightgbm

    params: dict[str, Any] = dict(config.LGBM_PARAMS)
    booster = lightgbm.train(params, lightgbm.Dataset(x, label=y), config.LGBM_ROUNDS)
    return lambda new: booster.predict(new)


def regression(train: pd.DataFrame, test: pd.DataFrame, h: int, fit) -> pd.DataFrame:
    keep_missing = fit is fit_lgbm
    out = []
    for side, label in (("long", f"nl_{h}"), ("short", f"ns_{h}")):
        rows = train.loc[train[label].notna()]
        x_train, stats = design(rows, None, keep_missing)
        predict = fit(x_train, clipped(rows[label]))
        x_test, _ = design(test, stats, keep_missing)
        out.append(
            pd.DataFrame(
                {
                    "decision_date": test["decision_date"],
                    "symbol": test["symbol"],
                    "side": side,
                    "score": predict(x_test),
                }
            )  # fmt: skip
        )
    return pd.concat(out, ignore_index=True)
