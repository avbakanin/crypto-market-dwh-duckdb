"""The forward test, registered on 2026-11-28 before any of its decision days happened.

Nothing here changes once the first decision is logged: a change is a new version with a
new name (and a new row in config/signal_definitions.csv). docs/forward_test.md.

Versions (both from the research version lgbm_hedged_h14, protocol 2):
- fwd_lgbm_hedged_h14: the research version as it is.
- fwd_lgbm_hedged_risk_h14: the same scores with risk rules against the tail the holdout
  showed (shorts of new, illiquid listings squeezed by hundreds of percent): no short of a
  contract with less than 180 days of history or outside the top 50 by turnover, at most
  10 open positions, and both legs closed when the pick moves 15 % against the entry.
The thresholds are round numbers chosen before the forward data, not fitted on the holdout
(the holdout is spent for this family; only the forward test can confirm a version).

Telegram calls every message a paper test until a version meets the criteria below.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

from research import config

REGISTERED_ON = date(2026, 11, 28)
# The first decision day logged (entry 2026-11-29 01:00 UTC).
FORWARD_START = date(2026, 11, 29)

# Model blocks: the first runs from FORWARD_START, the next ones start every half-year, as
# the research's walk-forward blocks did. A block's model trains on every member-day whose
# label exits TRAIN_LABEL_LAG_DAYS before the block starts: the archive publishes a day the
# morning after and a label needs a day past its exit, so the newest labels are not there
# yet at the first decision of a block, and a later training would see more of them.
REFIT_MONTHS = (1, 7)
TRAIN_LABEL_LAG_DAYS = 5
# The newest training decision day must have labels for at least this many members, or
# the model is not trained (the labels are late: a data problem, not a reason to train on
# fewer days).
MIN_LABELLED_MEMBERS = 90

HEDGE_SYMBOL = "BTCUSDT"

# Criteria to drop the "paper test" mark (all three, per version).
CRITERIA_MIN_DAYS = 183
CRITERIA_MIN_CLOSED = 100
# ... and the lower bound of the 95 % interval of the mean net per trade above 0.


@dataclass(frozen=True)
class RiskRules:
    short_min_history_days: int | None = None
    short_max_turnover_rank: int | None = None
    max_open: int | None = None
    stop_loss: float | None = None  # adverse move of the pick that closes both legs


@dataclass(frozen=True)
class ForwardVersion:
    name: str
    research: config.Version
    rules: RiskRules = field(default_factory=RiskRules)

    @property
    def horizon(self) -> int:
        return self.research.horizon

    @property
    def model_key(self) -> str:
        """Versions with the same family and horizon share their models."""
        return f"{self.research.family}_h{self.research.horizon}"


LGBM_HEDGED_H14 = config.Version("lgbm", 14, "hedged", protocol=2)

VERSIONS = [
    ForwardVersion("fwd_lgbm_hedged_h14", LGBM_HEDGED_H14),
    ForwardVersion(
        "fwd_lgbm_hedged_risk_h14",
        LGBM_HEDGED_H14,
        RiskRules(
            short_min_history_days=180,
            short_max_turnover_rank=50,
            max_open=10,
            stop_loss=0.15,
        ),
    ),
]


def block_start(day: date) -> date:
    """The start of the model block that decides `day`."""
    if day < FORWARD_START:
        raise ValueError(f"{day} is before the forward test ({FORWARD_START})")
    start = max(date(day.year, m, 1) for m in (1, *REFIT_MONTHS) if date(day.year, m, 1) <= day)
    return max(start, FORWARD_START)


def train_cut(block: date) -> date:
    """Training rows: decision_date + horizon < train_cut."""
    return block - timedelta(days=TRAIN_LABEL_LAG_DAYS)
