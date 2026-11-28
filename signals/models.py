"""Frozen models of the forward test: trained once per block, written once, never replaced.

A model file (data/signals/models/<model_key>_<block start>.json) holds both sides'
boosters (LightGBM text format) and the train fold's market-feature statistics, as
research.strategies.regression fits them; every logged decision carries the file's
SHA-256. The fit is the research's: the same features, targets (clipped net per side),
parameters and rounds, on every member-day whose label exits before the block's cut.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research import config, strategies
from signals import registry

SIDES = (("long", "nl"), ("short", "ns"))


class ModelError(RuntimeError):
    """A model cannot be trained or trusted; no decision is made with it."""


@dataclass(frozen=True)
class FrozenModel:
    key: str
    block: date
    sha256: str
    boosters: dict[str, Any]  # side -> lightgbm.Booster
    stats: dict[str, pd.DataFrame]  # side -> mean / std of the market features

    def score(self, panel: pd.DataFrame) -> pd.DataFrame:
        """Candidates (decision_date, symbol, side, score) of the panel's rows."""
        out = []
        for side, _ in SIDES:
            x, _ = strategies.design(panel, self.stats[side], keep_missing=True)
            names = self.boosters[side].feature_name()
            if list(x.columns) != list(names):
                raise ModelError(f"design columns differ from the model's ({side})")
            out.append(
                pd.DataFrame(
                    {
                        "decision_date": panel["decision_date"].to_numpy(),
                        "symbol": panel["symbol"].to_numpy(),
                        "side": side,
                        "score": self.boosters[side].predict(x),
                    }
                )
            )
        return pd.concat(out, ignore_index=True)


def model_path(models_dir: Path, key: str, block: date) -> Path:
    return models_dir / f"{key}_{block.isoformat()}.json"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def training_rows(panel: pd.DataFrame, h: int, cut: date) -> pd.DataFrame:
    """Member-days whose h-day label exits before `cut`; refused when the newest of those
    days lacks labels (the archive is late)."""
    exit_day = panel["decision_date"] + pd.Timedelta(days=h)
    rows = panel.loc[exit_day < pd.Timestamp(cut)]
    if rows.empty:
        raise ModelError(f"no training rows before {cut}")
    newest = rows["decision_date"].max()
    labelled = rows.loc[rows["decision_date"] == newest, f"nl_{h}"].notna().sum()
    if labelled < registry.MIN_LABELLED_MEMBERS:
        raise ModelError(
            f"{newest.date()} has labels for {labelled} members (h={h}), "
            f"fewer than {registry.MIN_LABELLED_MEMBERS}: the labels are late"
        )
    return rows


def train(panel: pd.DataFrame, version: registry.ForwardVersion, block: date) -> dict:
    """The model document of a block: boosters and statistics per side."""
    import lightgbm

    if version.research.family != "lgbm":
        raise ModelError(f"only lgbm models are frozen, not {version.research.family}")
    h = version.horizon
    cut = registry.train_cut(block)
    rows = training_rows(panel, h, cut)
    sides: dict[str, Any] = {}
    for side, prefix in SIDES:
        labelled = rows.loc[rows[f"{prefix}_{h}"].notna()]
        x, stats = strategies.design(labelled, None, keep_missing=True)
        booster = lightgbm.train(
            dict(config.LGBM_PARAMS),
            lightgbm.Dataset(x, label=strategies.clipped(labelled[f"{prefix}_{h}"])),
            config.LGBM_ROUNDS,
        )
        sides[side] = {
            "rows": len(labelled),
            "stats": {
                "mean": stats["mean"].astype(float).to_dict(),
                "std": stats["std"].astype(float).to_dict(),
            },
            "booster": booster.model_to_string(),
        }
    return {
        "model_key": version.model_key,
        "block_start": block.isoformat(),
        "train_cut": cut.isoformat(),
        "first_train_decision": str(rows["decision_date"].min().date()),
        "last_train_decision": str(rows["decision_date"].max().date()),
        "features": [*strategies.RANKS, *config.MARKET_FEATURES],
        "lightgbm": lightgbm.__version__,
        "sides": sides,
    }


def write_once(path: Path, document: dict) -> str:
    """Write the model file unless it exists (then it is kept as it is); its SHA-256."""
    if not path.exists():
        body = (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()
        create_once(path, body)
    return sha256_file(path)


def create_once(path: Path, body: bytes) -> None:
    """Write a new file whole or not at all: a temporary file renamed into place. Refused
    when the file exists (one writer: the pipeline runs one task at a time)."""
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_bytes(body)
    try:
        os.rename(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def load(path: Path) -> FrozenModel:
    import lightgbm

    document = json.loads(path.read_text(encoding="utf-8"))
    boosters, stats = {}, {}
    for side, _ in SIDES:
        part = document["sides"][side]
        boosters[side] = lightgbm.Booster(model_str=part["booster"])
        # The file's keys are sorted; the research's order matters: DataFrame - Series
        # with another label order sorts the columns, and the booster reads by position.
        stats[side] = pd.DataFrame(part["stats"]).astype(float).reindex(config.MARKET_FEATURES)
        expected = [*strategies.RANKS, *config.MARKET_FEATURES]
        if list(boosters[side].feature_name()) != expected:
            raise ModelError(f"{path.name}: features differ from the research's")
    return FrozenModel(
        key=document["model_key"],
        block=date.fromisoformat(document["block_start"]),
        sha256=sha256_file(path),
        boosters=boosters,
        stats=stats,
    )


def ensure(
    models_dir: Path,
    version: registry.ForwardVersion,
    block: date,
    panel_loader,
) -> FrozenModel:
    """The block's model: loaded when its file exists, else trained (panel_loader() gives
    the labelled panel) and written once."""
    path = model_path(models_dir, version.model_key, block)
    if not path.exists():
        write_once(path, train(panel_loader(), version, block))
    model = load(path)
    if not np.isfinite(model.stats["long"].to_numpy()).all():
        raise ModelError(f"{path.name}: statistics are not finite")
    return model
