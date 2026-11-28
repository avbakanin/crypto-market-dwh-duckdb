# Forward test (stage 4), registered 2026-11-28

Stage 3 confirmed no signal version (`docs/signal_research.md`): `lgbm_hedged_h14` passed
the walk-forward protocol and failed on the holdout, which is now spent. The only honest
test left is the future: decisions logged before the prices they bet on exist. Code:
`signals/`; daily task `signals` in the DAG; results in `marts.fct_signal_log` and
`marts.mart_signal_performance`, the dashboard's "Signals (paper test)" tab and Telegram.

## Registered versions (`signals/registry.py`, seed rows with status `forward_test`)

| Version | Rules |
|---|---|
| `fwd_lgbm_hedged_h14` | `lgbm_hedged_h14` as it is: LightGBM scores of the long and short net return at 14 days, the best candidate with a score > 0 a day, no second position in a symbol, BTCUSDT hedge of beta_btc_30d (clipped to [0, 3]) times the notional |
| `fwd_lgbm_hedged_risk_h14` | the same scores and model; no short of a contract with < 180 days of history or turnover rank > 50; at most 10 open positions; both legs closed when the pick moves 15 % against the entry |

The risk rules answer the holdout's tail (shorts of new, illiquid listings squeezed by
hundreds of percent); their thresholds are round numbers set before the forward data, not
fitted on the holdout. Either version changes only as a new version with a new name.

## How a day is decided (`python -m signals.run`)

- **When:** the first DAG run after 00:00 UTC (00:07; 01:07 UTC+1), after `dbt build` and
  the DQ log check passed. Entry is the open of the 01:00 UTC candle, as in the labels.
- **Data:** the features of D (`int_um__features_daily`, days up to D-1) must be complete:
  100 members, every hour of D-1 (`hours_feature_day = 24`), BTCUSDT among them. At 00:07
  D-1 comes from the API tail (the archive publishes it later that morning); later builds
  replace it with the archive's numbers, so a logged decision is not recomputed, it is the
  record.
- **Model:** frozen per block. The first block starts 2026-11-29, the next ones on
  2027-01-01, 2027-07-01, ... (the research's half-years). A block's model trains on every
  member-day whose label exits 5 days before the block (the newest labels are late) with
  the research's features, targets and parameters, is written once to
  `data/signals/models/<key>_<block>.json` and never replaced; each decision carries its
  SHA-256. The first model was trained on 2026-11-28, before any forward data:
  `lgbm_h14_2026-11-29.json`, SHA-256 `ef26b803e3b7f51b63189e50db18b35d027502e1a80a8df495ab740e156eadf2`
  (223,790 member-days per side, decisions 2020-02-01 .. 2026-11-09). Its scores equal the
  research's own fit on the same rows (difference 0.0 on the real panel; note 141).
- **Log:** `data/signals/log/<D>.json`, one file per day with every version's decision
  (trade with symbol, side, hedge, stop and the top 5 candidates; no trade and why; or
  missed). Written once, before the entry; each file holds the SHA-256 of the previous
  day's file; days have no gaps from 2026-11-29. A day the pipeline did not decide before
  01:00 UTC is logged as missed by the next run (a later decision could see the entry
  price). `signals.journal.verify` checks the chain before every run, the dbt test
  `assert_signal_log_chain_intact` on every build, and each Telegram message quotes the
  day's hash (an outside, timestamped copy).
- **Telegram:** every message starts with "PAPER TEST" and carries the research numbers,
  the forward results so far, the data and DQ state and the log hash.

## Outcomes and results

`fct_signal_log` closes a trade once `fct_um_forward_returns` labels its horizon for the
pick and the hedge (about 16 days after the decision), with the labels' entry, exit,
funding and costs. The stop (risk version) uses the hourly candles: the first hour whose
low (long) or high (short) crosses the stop closes the pick at the stop price, or at that
hour's open when it gapped through, and the hedge at that hour's close; funding up to the
hour's open. `mart_signal_performance` per version: logged / missed / no-trade days,
open and closed trades, hit rate, mean net with a normal 95 % interval (a first look: it
ignores the overlap of 14-day holdings), excess over the day's random member, survival of
3x leverage, worst trade.

## Criteria to drop the paper mark

All of: running >= 183 days, >= 100 closed trades, and the 95 % interval of the mean net
per trade above 0 — checked with the research's block bootstrap (`research.stats`, blocks
of 14 days) before the mark is dropped, not only the mart's normal interval. Until then a
message is information, not a recommendation to trade.
