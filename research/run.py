"""Run the research protocol: every version walk-forward, or the holdout of chosen ones.

Run from repo root, between pipeline runs (the warehouse is opened read-only):
    python -m research.run                    walk-forward on 2020 .. HOLDOUT_START
    python -m research.run --placebo          the same with labels shuffled within each
                                              day: excess returns must vanish
    python -m research.run --holdout V [V..]  train on everything before HOLDOUT_START,
                                              test once on the holdout; refused for a
                                              version already opened (holdout_log.jsonl)
Output: data/research/<run_id>/ trades.parquet, summary.csv, folds.csv, manifest.json.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from research import config, data, folds, selection, stats, strategies

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "research"
HOLDOUT_LOG = OUTPUT_DIR / "holdout_log.jsonl"


def evaluate(
    trades: pd.DataFrame, panel: pd.DataFrame, h: int, mode: str = "plain"
) -> pd.DataFrame:
    """Trades with their labels: net, MAE and the same day's random-member baseline.
    hedged: plus beta x the opposite BTCUSDT trade (net and MAE added, both per unit of
    the pick's notional; exposure = 1 + beta). The pair's MAE is bounded by the sum of the
    legs' MAEs (their worst hours need not coincide): conservative."""
    columns = ["decision_date", "symbol", f"nl_{h}", f"ns_{h}", f"mael_{h}", f"maes_{h}",
               f"base_nl_{h}", f"base_ns_{h}", "beta_btc_30d", f"btc_nl_{h}", f"btc_ns_{h}",
               f"btc_mael_{h}", f"btc_maes_{h}"]  # fmt: skip
    merged = trades.merge(panel[columns], on=["decision_date", "symbol"], how="left")
    long = merged["side"] == "long"
    merged["net"] = np.where(long, merged[f"nl_{h}"], merged[f"ns_{h}"])
    merged["mae"] = np.where(long, merged[f"mael_{h}"], merged[f"maes_{h}"])
    merged["base"] = np.where(long, merged[f"base_nl_{h}"], merged[f"base_ns_{h}"])
    merged["excess"] = merged["net"] - merged["base"]
    merged["exposure"] = 1.0
    if mode == "hedged":
        beta = merged["beta_btc_30d"].clip(*config.BETA_CLIP).fillna(1.0)
        hedge_net = np.where(long, merged[f"btc_ns_{h}"], merged[f"btc_nl_{h}"])
        hedge_mae = np.where(long, merged[f"btc_maes_{h}"], merged[f"btc_mael_{h}"])
        merged["net"] = merged["net"] + beta * hedge_net
        merged["mae"] = merged["mae"] + beta * hedge_mae
        merged["exposure"] = 1.0 + beta
    out = merged[["decision_date", "symbol", "side", "score", "net", "mae", "exposure", "base",
                  "excess"]]  # fmt: skip
    return out.loc[out["net"].notna()]


def run_version(
    version: config.Version, panel: pd.DataFrame, fold_list: list[folds.Fold]
) -> pd.DataFrame:
    h = version.horizon
    parts = []
    for fold, train, test in folds.walk_forward(panel, fold_list, h):
        scored = strategies.candidates(version.family, train, test, h)
        scored = strategies.apply_mode(scored, test, version.mode)
        trades = evaluate(selection.select_trades(scored, h), panel, h, version.mode)
        trades.insert(0, "fold", fold.name)
        parts.append(trades)
    trades = pd.concat(parts, ignore_index=True)
    trades["decision_date"] = pd.to_datetime(trades["decision_date"])
    trades.insert(0, "version", version.name)
    return trades


def max_concurrent(trades: pd.DataFrame, h: int) -> int:
    if trades.empty:
        return 0
    days = np.sort(trades["decision_date"].to_numpy())
    ends = days + np.timedelta64(h, "D")
    return int(max(((days <= d) & (ends > d)).sum() for d in days))


def summarize(
    version: config.Version, trades: pd.DataFrame, fold_list: list[folds.Fold]
) -> tuple[dict, list[dict]]:
    h = version.horizon
    start, end = fold_list[0].test_start, fold_list[-1].test_end
    per_day = stats.daily_series(trades, "net", start, end)
    mean, low, high, p = stats.block_bootstrap_mean(
        per_day, h, config.BOOTSTRAP_SAMPLES, config.SEED
    )
    excess_day = stats.daily_series(trades, "excess", start, end)
    ex_mean, ex_low, ex_high, ex_p = stats.block_bootstrap_mean(
        excess_day, h, config.BOOTSTRAP_SAMPLES, config.SEED + 1
    )
    fold_rows = []
    for fold in fold_list:
        part = trades[trades["fold"] == fold.name]
        fold_rows.append(
            {"version": version.name, "fold": fold.name, "trades": len(part),
             "mean_net": part["net"].mean(), "mean_excess": part["excess"].mean(),
             "hit_rate": (part["net"] > 0).mean() if len(part) else np.nan}
        )  # fmt: skip
    positive = sum(1 for r in fold_rows if r["trades"] and r["mean_net"] > 0)
    survive = trades["mae"] > -trades["exposure"] * (1 / 3 - config.MAINTENANCE_MARGIN)
    years = trades["decision_date"].dt.year
    by_year = {int(y): float(trades.loc[years == y, "net"].mean()) for y in sorted(set(years))}
    row = {
        "version": version.name,
        "family": version.family,
        "mode": version.mode,
        "protocol": version.protocol,
        "horizon": h,
        "trades": len(trades),
        "longs": int(np.sum(trades["side"].to_numpy() == "long")),
        "hit_rate": (trades["net"] > 0).mean(),
        "mean_net": mean,
        "ci_low": low,
        "ci_high": high,
        "p": p,
        "median_net": trades["net"].median(),
        "mean_excess": ex_mean,
        "excess_ci_low": ex_low,
        "excess_ci_high": ex_high,
        "excess_p": ex_p,
        "positive_folds": positive,
        "survive_3x": survive.mean() if len(trades) else np.nan,
        "mae_p10": trades["mae"].quantile(0.10) if len(trades) else np.nan,
        "max_concurrent": max_concurrent(trades, h),
        **{f"mean_{y}": v for y, v in by_year.items()},
    }
    return row, fold_rows


def admit(summary: pd.DataFrame) -> pd.DataFrame:
    summary = summary.copy()
    summary["p_holm"] = stats.holm(summary["p"].tolist())
    summary["admitted"] = (
        (summary["p_holm"] < config.ALPHA)
        & (summary["trades"] >= config.MIN_TRADES)
        & (summary["positive_folds"] >= config.MIN_POSITIVE_FOLDS)
        & (summary["survive_3x"] >= config.MIN_SURVIVE_3X)
    )
    return summary


def shuffle_labels_within_day(panel: pd.DataFrame, seed: int) -> pd.DataFrame:
    """Placebo: each day's label rows permuted across that day's members."""
    label_columns = [
        f"{name}_{h}"
        for h in config.HORIZONS
        for name in data.LABEL_COLUMNS
        if f"{name}_{h}" in panel
    ]
    rng = np.random.default_rng(seed)
    shuffled = panel.copy()
    for _, index in panel.groupby("decision_date").groups.items():
        positions = np.asarray(index)
        shuffled.loc[positions, label_columns] = panel.loc[
            rng.permutation(positions), label_columns
        ].to_numpy()
    return shuffled


def git_state() -> dict:
    def run(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=PROJECT_ROOT, capture_output=True, text=True, check=False
        ).stdout.strip()

    return {"commit": run("rev-parse", "HEAD"), "dirty": bool(run("status", "--porcelain"))}


def opened_versions(log: Path | None = None) -> set[str]:
    log = log or HOLDOUT_LOG
    if not log.exists():
        return set()
    return {json.loads(line)["version"] for line in log.read_text().splitlines() if line}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Signal research walk-forward.")
    parser.add_argument("--placebo", action="store_true")
    parser.add_argument("--holdout", nargs="+", metavar="VERSION")
    parser.add_argument(
        "--warehouse", type=Path, default=PROJECT_ROOT / "warehouse" / "crypto_dwh.duckdb"
    )
    args = parser.parse_args(argv)

    by_name = {v.name: v for v in config.VERSIONS}
    if args.holdout:
        unknown = [v for v in args.holdout if v not in by_name]
        if unknown:
            parser.error(f"unknown versions: {unknown}")
        again = sorted(set(args.holdout) & opened_versions())
        if again:
            parser.error(f"holdout already opened for {again}: it is looked at once")
        versions = [by_name[v] for v in args.holdout]
    else:
        versions = config.VERSIONS

    features, labels = data.read_warehouse(args.warehouse)
    panel = data.build_panel(features, labels, include_holdout=bool(args.holdout))
    if args.placebo:
        panel = shuffle_labels_within_day(panel, config.SEED)
    last_day = panel["decision_date"].max() + pd.Timedelta(days=1)
    fold_list = (
        folds.make_folds([config.HOLDOUT_START], last_day.date())
        if args.holdout
        else folds.make_folds(config.FOLD_STARTS, config.HOLDOUT_START)
    )

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    kind = "holdout" if args.holdout else ("placebo" if args.placebo else "walk_forward")
    out = OUTPUT_DIR / f"{run_id}_{kind}"
    out.mkdir(parents=True)
    all_trades, rows, fold_rows = [], [], []
    for version in versions:
        trades = run_version(version, panel, fold_list)
        row, per_fold = summarize(version, trades, fold_list)
        print(
            f"{version.name}: {row['trades']} trades, "
            f"mean net {row['mean_net']:.4f}, p {row['p']:.4f}"
        )
        all_trades.append(trades)
        rows.append(row)
        fold_rows += per_fold
    summary = admit(pd.DataFrame(rows))
    pd.concat(all_trades, ignore_index=True).to_parquet(out / "trades.parquet")
    summary.to_csv(out / "summary.csv", index=False)
    pd.DataFrame(fold_rows).to_csv(out / "folds.csv", index=False)
    manifest = {
        "run_id": run_id,
        "kind": kind,
        "versions": [v.name for v in versions],
        "folds": [f.name for f in fold_list],
        "holdout_start": config.HOLDOUT_START.isoformat(),
        "panel_last_decision": str(panel["decision_date"].max().date()),
        "git": git_state(),
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    if args.holdout:
        with HOLDOUT_LOG.open("a") as log:
            for version in versions:
                log.write(json.dumps({"version": version.name, "run_id": run_id}) + "\n")
    print(f"written: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
