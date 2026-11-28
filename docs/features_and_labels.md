# Features and labels (USD-M, daily)

The research table of the signal system: one row per decision day D and member of the
point-in-time universe of D (`int_um__universe_daily`, top 100 by turnover over D-31..D-2),
with features known at 00:07 UTC of D and labels of trades entered right after.

## Timing

| Moment | UTC | What |
|---|---|---|
| D-1 23:00-00:00 | last hourly candle the features read | `int_um__features_daily.feature_date` = D-1 |
| D 00:07 | pipeline run that computes the signal of D | done ~00:15 |
| D 01:00 | entry: open of the hourly candle | `entry_delay_hours: 1` |
| D+h 01:00 | exit: open of the hourly candle, h in 1, 3, 7, 14 days | `label_horizons` |

## Features (`int_um__features_daily`)

From `int_um__daily_bars` (UTC daily bars built from the hourly candles, funding and
metrics). Windows are trailing RANGE frames on the date (`macros/um_windows.sql`): a
missing day stays missing, a window with fewer days than its length gives NULL.

| Group | Columns | From |
|---|---|---|
| momentum | `ret_1d` .. `ret_60d` (close to close) | 2020-02 |
| trend | `close_to_sma20`, `close_to_sma50` | |
| volatility | `rv_7d`, `rv_30d` (daily, from hourly log returns), `atr_14d_pct` | |
| turnover, taker | `avg_turnover_30d`, `turnover_z_30d` (log), `taker_buy_share_1d/7d` | |
| funding | `funding_1d`, `funding_7d` (sums of rates), `funding_z_30d` | |
| open interest | `oi_usdt` (D-1 23:00), `oi_chg_1d/7d`, `oi_to_turnover_7d` | 2021-12 |
| long/short | `global_ls`, `global_ls_chg_7d` | 2022-02 |
| | `top_position_ls`, `top_account_ls` | 2023-01 |
| vs BTC | `rel_btc_ret_7d/30d`, `beta_btc_30d`, `btc_ret_*`, `btc_close_to_sma50` | |
| vs members | `ret_7d_rank_pct`, `ret_30d_rank_pct` | |
| breadth | `breadth_above_sma20`, `breadth_ret_7d_median` | |
| quality | `hours_feature_day`, `history_days`, `turnover_rank` | |

Members: ~35 in 2020, ~95 in 2021, 100 from 2022. No mark/index premium yet.

## Labels (`fct_um_forward_returns`)

Per (D, symbol, horizon), for members whose entry candle traded:

- `gross_return` = exit / entry - 1.
- `funding_return`: sum of funding rates settled in (entry, exit], on the entry notional.
  The long pays it, the short receives it (a negative rate reverses both).
- `cost_return`: round trip, 2 x (taker fee + slippage). Taker fee: 5 bps (VIP 0, no BNB
  discount, checked 2026-11-28). Slippage per side: max(2 bps, rv_30d x sqrt(order /
  avg_turnover_30d)) with order = $10k (square-root impact, inputs from the features of D).
  Median round trip 23.6 bps; the 10 most liquid 14 bps.
- `net_return_long` = gross - funding - cost; `net_return_short` = -gross + funding - cost.
- `mae_*` / `mfe_*`: worst / best excursion of traded candles from entry to target.
  `*_survives_<x>x`: MAE above -(1/x - 1 % maintenance margin): no liquidation.
- Exit: the target candle, or the next traded candle within a day (archive gaps). No traded
  candle from the target to a day after it means delisted or halted: exit at the close of
  the last traded candle before the target, `exit_reason = 'delisted'` (758 rows, 36
  contracts: LUNA, FTT, ALPACA's +488 % delisting squeeze). These rows stay: dropping them
  would bias every backtest.
- A horizon is labelled once the archive covers a day past its target (~2 days lag).

## Checks

- Unit tests: features of D do not change when every input from D on changes (mutants with
  lead, a following frame or D's own bar fail); labels against hand-computed costs, funding
  sign, delisting exit and MAE (mutants with a flipped funding sign or a path past the
  target fail).
- Stop: core features not NULL where 31+ days of history and a full D-1; every tradable
  member has a label for every covered horizon; MAE <= 0 <= MFE; cost within 14-500 bps;
  funding on every recent member day.
- Warn: daily bars vs the archive's own daily candles (source note 139).

## Liquidations (collected from 2026-11-28, not a feature yet)

`int_um__liquidations_daily`: long / short liquidated notional and counts per (contract,
UTC day) from the all-market stream (`ingestion/collect_liquidations.py`, source note 142),
with `coverage_minutes` of the day (1440 = complete). The stream sends at most one
liquidation per contract per second, so the volume is a lower bound. A feature needs months
of history first; days before 2026-11-28 have none.

## Market structure (from 2026-11-28, not a feature yet)

Inputs for the next features (source note 143); none feeds a registered version.

- `int_um__premium_index_1h` / `int_um__premium_daily`: the premium of the contract over
  its index (the unclamped basis behind funding), hourly from the archive since 2020 and
  from the API tail after it; per day the mean of hourly closes, the last close and the
  range. Live by the 00:07 run.
- `int_um__spot_daily`: the coin's spot pair, matched by price, per contract day: spot
  close per contract unit, spot quote and taker buy volume, `spot_perp_log_gap`, and
  `price_matches` (false on days more than 10 % apart or with either side idle).
- `int_um__book_depth_daily`: medians over the day's hourly snapshots of the USDT depth
  within 0.2 / 1 / 2 % of the mid per side, and the 1 % imbalance, from 2023.
- Spot and book depth come from the archive only: day D-1 is published in the morning of
  D, after the 01:00 entry, so a feature of D must read D-2 at the latest (in research too,
  or the backtest sees a day the live signal does not).
- Warn checks: premium archive vs API, premium and book depth member-day coverage, spot
  days more than 10 % off their contract.
