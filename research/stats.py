"""Inference on trade returns: block bootstrap over days, Holm's correction."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd


def as_date(value: object) -> date:
    """A date from a date, datetime, Timestamp or ISO string."""
    return date.fromisoformat(str(value)[:10])


def daily_series(trades: pd.DataFrame, value: str, start: date, end: date) -> list[list[float]]:
    """Per calendar day of [start, end): the list of trade values decided that day."""
    by_day: dict[date, list[float]] = {}
    for day, v in zip(trades["decision_date"], trades[value], strict=True):
        by_day.setdefault(as_date(day), []).append(float(v))
    n = (end - start).days
    return [by_day.get(start + timedelta(days=i), []) for i in range(n)]


def block_bootstrap_mean(
    per_day: list[list[float]], block: int, samples: int, seed: int
) -> tuple[float, float, float, float]:
    """Mean of all trade values, its 95 % percentile interval and a one-sided p-value of
    mean <= 0, by a circular block bootstrap over days (blocks of `block` days keep the
    dependence of overlapping holding periods and of market regimes within a block).
    p = share of resampled means, centred on the sample mean, at or above the mean."""
    sums = np.array([sum(v) for v in per_day], dtype=float)
    counts = np.array([len(v) for v in per_day], dtype=float)
    total = counts.sum()
    if total == 0:
        return float("nan"), float("nan"), float("nan"), float("nan")
    mean = sums.sum() / total
    n = len(per_day)
    block = max(1, min(block, n))
    rng = np.random.default_rng(seed)
    blocks_needed = int(np.ceil(n / block))
    starts = rng.integers(0, n, size=(samples, blocks_needed))
    offsets = np.arange(block)
    index = ((starts[:, :, None] + offsets[None, None, :]) % n).reshape(samples, -1)[:, :n]
    boot_counts = counts[index].sum(axis=1)
    boot_means = np.where(boot_counts > 0, sums[index].sum(axis=1) / np.maximum(boot_counts, 1), 0)
    low, high = np.percentile(boot_means, [2.5, 97.5])
    p = float(np.mean(boot_means - mean >= mean))
    return float(mean), float(low), float(high), p


def holm(p_values: list[float]) -> list[float]:
    """Holm-adjusted p-values (family-wise error), same order as given; NaN stays NaN."""
    p = np.array(p_values, dtype=float)
    valid = ~np.isnan(p)
    adjusted = np.full_like(p, np.nan)
    order = np.argsort(p[valid])
    values = p[valid][order]
    m = len(values)
    running = 0.0
    out = np.empty(m)
    for i, value in enumerate(values):
        running = max(running, (m - i) * value)
        out[i] = min(1.0, running)
    result = np.empty(m)
    result[order] = out
    adjusted[valid] = result
    return adjusted.tolist()
