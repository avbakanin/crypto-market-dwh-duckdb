"""The research protocol, fixed before any result is seen: periods, folds, versions, rules.

Changing anything here after a run is a new protocol; the run manifest records it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

# Labels of decisions from this day on are the final holdout: never read by a research
# run (research.data drops them), opened once per version by --holdout (research.run).
HOLDOUT_START = date(2025, 7, 1)

# Walk-forward test blocks (half-years), each trained on everything before it.
FOLD_STARTS = [
    date(2022, 1, 1),
    date(2022, 7, 1),
    date(2023, 1, 1),
    date(2023, 7, 1),
    date(2024, 1, 1),
    date(2024, 7, 1),
    date(2025, 1, 1),
]

# Horizons researched (days). 1 day is left out: its round trip (~0.27 %) exceeds every
# 1-day effect of the pre-check (docs/signal_research.md).
HORIZONS = [3, 7, 14]

# Features ranked across the members of each day (centred percent ranks).
SYMBOL_FEATURES = [
    "ret_1d", "ret_3d", "ret_7d", "ret_14d", "ret_30d", "ret_60d",
    "close_to_sma20", "close_to_sma50", "rv_7d", "rv_30d", "atr_14d_pct",
    "avg_turnover_30d", "turnover_z_30d", "taker_buy_share_1d", "taker_buy_share_7d",
    "funding_1d", "funding_7d", "funding_z_30d", "oi_chg_1d", "oi_chg_7d",
    "oi_to_turnover_7d", "global_ls", "global_ls_chg_7d", "top_position_ls",
    "top_account_ls", "rel_btc_ret_7d", "rel_btc_ret_30d", "beta_btc_30d",
]  # fmt: skip

# Features of the whole market on the day (same for every member), standardized.
MARKET_FEATURES = [
    "btc_close_to_sma50", "btc_ret_7d", "btc_ret_30d", "breadth_above_sma20",
    "breadth_ret_7d_median", "market_funding_1d",
]  # fmt: skip

# Admission of a version (all on the walk-forward test blocks).
ALPHA = 0.05  # family-wise, Holm over every version of the run
MIN_TRADES = 100
MIN_POSITIVE_FOLDS = 5  # of the 7 blocks
MIN_SURVIVE_3X = 0.90  # share of trades whose MAE stays above -exposure x (1/3 - 1 %)
MAINTENANCE_MARGIN = 0.01

# Holdout (declared before it is opened): an admitted version passes when its mean net
# over the holdout is above 0; it is confirmed when the one-sided bootstrap p is also
# below HOLDOUT_ALPHA. Only admitted versions are opened, each once.
HOLDOUT_ALPHA = 0.05

BOOTSTRAP_SAMPLES = 10_000
SEED = 20261128


@dataclass(frozen=True)
class Version:
    """A signal version: a family of rules or models, its horizon and its mode.

    Modes (protocol v2, declared 2026-11-28 after v1 admitted none, before any v2 run):
    - plain: the trade as selected;
    - hedged: the selected trade plus the opposite BTCUSDT trade of beta_btc_30d (D-1,
      clipped to [0, 3]) times its notional; BTCUSDT itself is never selected;
    - regime: longs only while BTC closes above its 50-day SMA, shorts only below.
    """

    family: str  # btc_trend | composite | ridge | lgbm
    horizon: int
    mode: str = "plain"
    protocol: int = 1

    @property
    def name(self) -> str:
        middle = "" if self.mode == "plain" else f"_{self.mode}"
        return f"{self.family}{middle}_h{self.horizon}"


FAMILIES = ["btc_trend", "composite", "ridge", "lgbm"]
VERSIONS_V1 = [Version(family, h) for family in FAMILIES for h in HORIZONS]
# v1 verdict (run 20261128T100304Z): none admitted; ridge/lgbm h14 pick coins better than
# the day's random member (excess p 0.0002 / 0.0035, placebo ~0), but market direction
# and the path drawdown sink the absolute return. v2 isolates the selection (hedged) or
# adds the BTC trend (regime). Holm runs over v1 and v2 together.
VERSIONS_V2 = [
    Version(family, h, mode, protocol=2)
    for mode in ("hedged", "regime")
    for family in ("ridge", "lgbm")
    for h in (7, 14)
]
VERSIONS = VERSIONS_V1 + VERSIONS_V2
BETA_CLIP = (0.0, 3.0)


# Fixed model settings (no tuning on the test blocks).
RIDGE_ALPHA = 1000.0
WINSOR = 0.01  # targets clipped at the train fold's 1st / 99th percentile
LGBM_ROUNDS = 300
LGBM_PARAMS = {
    "objective": "regression",
    "learning_rate": 0.03,
    "num_leaves": 15,
    "min_data_in_leaf": 500,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "feature_fraction": 0.8,
    "lambda_l2": 10.0,
    "seed": SEED,
    "deterministic": True,
    "force_row_wise": True,
    "num_threads": 4,
    "verbose": -1,
}
# composite: features whose train IC has |Newey-West t| above this enter the score.
COMPOSITE_MIN_T = 2.0
