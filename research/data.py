"""The research panel: one row per (decision_date, member) with features and labels.

Labels are wide, per horizon h: g_h (gross), nl_h / ns_h (net long / short), mael_h /
maes_h (MAE), the day's equal-weight average of nl_h / ns_h over the members (the
random-member baseline of that day and side), and the day's BTCUSDT labels (btc_*). A
research run drops every row decided on or after HOLDOUT_START and every label whose exit
reaches it.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd

from research.config import HOLDOUT_START, HORIZONS, MARKET_FEATURES, SYMBOL_FEATURES

LABEL_COLUMNS = {
    "g": "gross_return",
    "nl": "net_return_long",
    "ns": "net_return_short",
    "mael": "mae_long",
    "maes": "mae_short",
}


def read_warehouse(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Features and labels from the warehouse, read-only (fails while the pipeline writes)."""
    import duckdb

    with duckdb.connect(str(path), read_only=True) as con:
        con.execute("set TimeZone = 'UTC'")
        features = con.execute("select * from staging.int_um__features_daily").df()
        labels = con.execute(
            "select decision_date, symbol, horizon_days, "
            + ", ".join(f"{c}::double as {c}" for c in LABEL_COLUMNS.values())
            + " from marts.fct_um_forward_returns where horizon_days in ("
            + ", ".join(str(h) for h in HORIZONS)
            + ")"
        ).df()
    return features, labels


def feature_panel(features: pd.DataFrame, extra: tuple[str, ...] = ()) -> pd.DataFrame:
    """Features with the day's centred ranks and the market features; `extra` columns of
    the features are carried along unchanged."""
    columns = ["decision_date", "symbol", *SYMBOL_FEATURES, *MARKET_FEATURES[:-1]]
    extra_columns = [c for c in extra if c not in columns]
    panel = cast(pd.DataFrame, features[[*columns, *extra_columns]]).copy()
    panel["decision_date"] = pd.to_datetime(panel["decision_date"])
    for column in [*SYMBOL_FEATURES, *MARKET_FEATURES[:-1]]:
        panel[column] = panel[column].astype(float)
    by_day = panel.groupby("decision_date")
    panel["market_funding_1d"] = by_day["funding_1d"].transform("median")
    for column in SYMBOL_FEATURES:
        panel[f"rank_{column}"] = by_day[column].rank(pct=True) - 0.5
    return panel


def build_panel(
    features: pd.DataFrame, labels: pd.DataFrame, include_holdout: bool
) -> pd.DataFrame:
    """Features with centred ranks, market features and wide labels."""
    panel = feature_panel(features)

    wide = labels.copy()
    wide["decision_date"] = pd.to_datetime(wide["decision_date"])
    wide = wide.rename(columns={v: k for k, v in LABEL_COLUMNS.items()})
    wide = wide.pivot_table(
        index=["decision_date", "symbol"], columns="horizon_days", values=list(LABEL_COLUMNS)
    )
    wide.columns = [f"{name}_{h}" for name, h in wide.columns]
    panel = panel.merge(wide.reset_index(), on=["decision_date", "symbol"], how="left")

    if not include_holdout:
        panel = drop_holdout(panel)
    for h in HORIZONS:
        for side in ("nl", "ns"):
            column = f"{side}_{h}"
            if column not in panel:
                panel[column] = np.nan
            panel[f"base_{column}"] = panel.groupby("decision_date")[column].transform("mean")
    # The day's BTCUSDT labels on every row: the hedge leg of the hedged mode.
    btc_columns = [f"{n}_{h}" for h in HORIZONS for n in ("nl", "ns", "mael", "maes")]
    btc_columns = [c for c in btc_columns if c in panel]
    btc = panel.loc[panel["symbol"] == "BTCUSDT", ["decision_date", *btc_columns]]
    panel = panel.merge(
        btc.rename(columns={c: f"btc_{c}" for c in btc_columns}), on="decision_date", how="left"
    )
    return panel.sort_values(["decision_date", "symbol"]).reset_index(drop=True)


def drop_holdout(panel: pd.DataFrame, start: date = HOLDOUT_START) -> pd.DataFrame:
    """Rows decided before `start`, with every label whose exit reaches `start` blanked."""
    cut = pd.Timestamp(start)
    kept = panel.loc[panel["decision_date"] < cut].copy()
    for h in HORIZONS:
        late = kept["decision_date"] + pd.Timedelta(days=h) >= cut
        for name in LABEL_COLUMNS:
            column = f"{name}_{h}"
            if column in kept:
                kept.loc[late, column] = np.nan
    return kept
