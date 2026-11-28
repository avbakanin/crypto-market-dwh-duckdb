# Signal research (stage 3), 2026-11-28

Question: does any rule or model on the daily features (`int_um__features_daily`) give
trades with a positive expected return after costs and funding (`fct_um_forward_returns`),
beyond chance? Code: `research/` (`python -m research.run`), runs in `data/research/`,
versions and their status in `config/signal_definitions.csv`.

**Verdict: no version is confirmed. Nothing is fit to trade with leverage yet.** One
version passed the walk-forward protocol and failed to confirm on the untouched holdout.

## Protocol (fixed before results, `research/config.py`)

- Data: decision days 2020-02 .. 2025-06-30 for research; **holdout 2025-07-01 .. now**
  (17 months) never read by a research run (labels whose exit reaches it are blanked).
- Walk-forward: expanding window, 7 half-year test blocks 2022H1 .. 2025H1; training
  rows only if their label exits before the block (embargo).
- Trades: at most one a day (best score > 0), no second position in a symbol while one is
  open. Horizons 3, 7, 14 days (1 day: costs exceed every 1-day effect).
- Inference: circular block bootstrap over days (block = horizon) for the mean net and
  its 95 % interval; Holm over every version of the protocol (v1 and v2: 20).
- Admission: Holm p < 0.05, >= 100 trades, positive in >= 5 of 7 blocks, >= 90 % of
  trades survive 3x leverage (MAE above -(1/3 - 1 %) of exposure).
- Holdout: admitted versions only, once each (`data/research/holdout_log.jsonl`); passes
  with mean net > 0, confirmed with one-sided p < 0.05.
- Placebo: labels shuffled within each day; the excess over the day's random member must
  vanish (it does: every cross-sectional version within -2.5 .. +1.1 %, none with p < 0.07).

## Pre-check (research period only)

- Random member, net per trade: 14 d long +1.44 %, short -1.98 %; driven by the year
  (2021 long +8.4 %, 2022 and 2025H1 long -4.6 / -6.0 %).
- Market timing: BTC above its 50-day SMA (top third of days) -> random 14 d long +8.1 %,
  short -8.7 %; other days long -1.5 .. -2.0 %.
- Cross-section: strongest rank ICs are volatility (rv/ATR, IC -0.09 .. -0.13, every year),
  OI to turnover (+0.07 .. +0.10), taker buy share 7 d (+0.03 at 14 d). Shorting the most
  volatile decile loses on average (squeezes) despite the rank IC.

## Results (walk-forward run `20261128T102702Z`, commit `1c392b3`)

| Version | Trades | Mean net | 95 % CI low | Holm p | + blocks | Survive 3x |
|---|---|---|---|---|---|---|
| btc_trend_h3 / h7 / h14 | 429 / 184 / 93 | +0.08 / +0.22 / -0.60 % | < 0 | 1.0 | 5 / 3 / 3 | 99-100 % |
| composite_h3 / h7 / h14 | ~1270 | -0.37 / -0.16 / +0.44 % | < 0 | 1.0 | 3 / 4 / 3 | 84-95 % |
| ridge_h14 | 1262 | +2.11 % | -0.23 % | 0.76 | 5 | 87 % |
| lgbm_h14 | 1258 | +2.36 % | -0.21 % | 0.63 | 6 | 84 % |
| ridge_hedged_h14 | 1262 | +1.08 % | -0.71 % | 1.0 | 7 | 99 % |
| **lgbm_hedged_h14** | 1258 | **+3.40 %** | **+1.46 %** | **0.012** | **7** | **97 %** |
| lgbm_regime_h14 | 779 | +2.59 % | -0.64 % | 1.0 | 4 | 84 % |

(all 20 in the run's `summary.csv`.) v1 (plain) admitted none: ridge and LightGBM at 14 d
pick coins better than the day's random member of the same side (excess +2.0 %, p 0.0002
and 0.0035, placebo ~0), but market direction and path drawdown sink the absolute return.
v2, declared after v1 and before its run: `hedged` (the pick plus beta x the opposite
BTCUSDT trade) and `regime` (sides follow the BTC trend).

lgbm_hedged_h14 decomposed: against the same hedge on the day's random member it earns
+3.1 % on longs and +1.5 % on shorts, positive in every year; of the shorts' +4.7 %, ~3.2 %
is the 2022-2025 drift of alts against BTC (in 2021 the same random hedged short lost
8.2 % per 14 days).

## Holdout (run `20261128T103727Z`, opened once)

lgbm_hedged_h14, 2025-07-01 .. 2026-11: 497 trades, mean net +0.65 % (pass by the letter),
95 % CI -4.7 .. +5.8 %, p 0.42 (**not confirmed**), excess over the random member -0.66 %,
survive 3x 86.5 %. Trade returns are twice as dispersed as in walk-forward (std 0.55 vs
0.30): shorts of illiquid new listings squeezed by hundreds of percent (PIPPINUSDT -677 %
in 14 days, VELVETUSDT, MUSDT) — 2025-2026 has the highest member volatility of the data.

## What it means and what follows

- The selection skill of v1/v2 was real on 2022-2025H1 and did not carry into the next 17
  months; the hedge worked partly through the alt-vs-BTC drift. Shorts of illiquid,
  newly listed coins are the main tail risk; a 14-day short without a stop is not a
  tradable instrument there.
- The holdout is spent for this version, and any version designed after looking at it
  carries that look. From here only a **forward test** (signals logged before the future
  happens, stage 4) can confirm a version honestly.
