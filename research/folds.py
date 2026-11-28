"""Walk-forward folds with an embargo: training labels end before the test block starts."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date

import pandas as pd


@dataclass(frozen=True)
class Fold:
    test_start: date
    test_end: date  # exclusive

    @property
    def name(self) -> str:
        return f"{self.test_start:%Y-%m}"


def make_folds(starts: list[date], end: date) -> list[Fold]:
    """Consecutive test blocks: each start up to the next one, the last up to `end`."""
    bounds = [*starts, end]
    return [Fold(a, b) for a, b in zip(bounds[:-1], bounds[1:], strict=True)]


def split(panel: pd.DataFrame, fold: Fold, horizon: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Train: rows whose h-day label exits before the test block (decision + h < start).
    Test: rows decided within the block. Labels of a test row may run past the block end;
    they are evaluated, never trained on, in this fold."""
    start, end = pd.Timestamp(fold.test_start), pd.Timestamp(fold.test_end)
    exit_day = panel["decision_date"] + pd.Timedelta(days=horizon)
    train = panel.loc[exit_day < start]
    test = panel.loc[(panel["decision_date"] >= start) & (panel["decision_date"] < end)]
    return train, test


def walk_forward(
    panel: pd.DataFrame, folds: list[Fold], horizon: int
) -> Iterator[tuple[Fold, pd.DataFrame, pd.DataFrame]]:
    for fold in folds:
        train, test = split(panel, fold, horizon)
        yield fold, train, test
