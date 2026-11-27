# What happens around a pump: findings

> Written on the Snowflake original ([crypto-market-dwh](https://github.com/avbakanin/crypto-market-dwh)).
> The port reproduces its marts on the same input ([duckdb_vs_snowflake_export.md](duckdb_vs_snowflake_export.md)),
> except the numbers against control hours, which depend on the drawn hours (source note 121).

The question of the project: **what do volume, open interest and funding do in the 24 hours
before and after a sharp hourly rise of a coin?** Below are five findings from a year of
hourly Binance data on 23 liquid coins, each with its sample size, a 95 % confidence
interval and its limits.

Numbers as of export `20261124T170603Z` (data to 2026-11-24 15:00 UTC). To recompute them
from the newest export, without Snowflake:

```powershell
pip install -r requirements-dashboard.txt
python -m analysis.pump_findings
```

## How the numbers are made

- **A pump** is an hour whose close is at least 5 % above the previous close; hours of one
  coin less than 24 h apart form one episode (`fct_pump_events`). The plan said +20 %, but
  among liquid coins that happened once in a year (p99.9 of hourly returns is +5.35 %), so
  the threshold follows the data. 198 episodes, 21 coins, 113 days
  (2025-11-21 … 2026-11-23).
- **The comparison** is with ordinary hours, not with zero: for each pump, 30 hours of the
  same coin with no hour past the threshold within 48 h, measured by the same SQL
  (`fct_pump_control_window`). The same coin, because three coins (ZEC, ENA, PUMP) have
  38 % of the pumps and their normal state is not the market's. **Excess** = the pump's
  value minus the median of its own 30 controls (`fct_pump_event_excess`).
- **Confidence intervals** come from a bootstrap over *days*, not events: pumps of one day
  move together (a market rally lifts ten coins in the same hour), and resampling single
  events would treat them as independent and narrow the interval. 2000 resamples, fixed
  seed.
- **Hour 0** is the pump hour; "vs the start" means against the close of hour −1.

## 1. Most pumps are a coin's own, but a third are market moves

| Scope | Pumps | Days | Coins |
|---|---|---|---|
| All | 198 | 113 | 21 |
| Single coin (no other coin past 5 % in that hour) | 120 | 100 | 20 |
| Market move (2+ coins in the same hour) | 78 | 21 | 18 |

The median pump hour is +5.9 %. Market moves are concentrated: 78 pumps on 21 days, which
is why every interval below resamples days.

## 2. The pump leaves a footprint in volume for about three hours before

| Before the start | Pump median | Controls' median | Median excess [95 % CI] |
|---|---|---|---|
| Volume / 7-day mean, hour −3 | 1.15× | 0.68× | +0.45 [+0.33; +0.67] |
| Volume / 7-day mean, hour −1 | 1.56× | 0.70× | +0.84 [+0.50; +1.23] |
| Volume / 7-day mean, pump hour | 4.49× | 0.69× | +3.73 [+2.94; +4.80] |
| Price 6 h before vs the start | −1.58 % | +0.08 % | −1.75 % [−2.18 %; −0.82 %] |
| Taker buy share, hour −1 | 50.4 % | 49.3 % | +0.7 p.p. [−0.5; +2.4] |
| Taker buy share, pump hour | 55.1 % | 49.6 % | +5.5 p.p. [+4.7; +6.6] |

194–198 pumps, 110–113 days. Volume runs above its weekly mean for the three hours before
the pump, while an ordinary hour sits at 0.7× (the mean is pulled up by a heavy tail), and
the price drifts up about 1.6 % over the six hours before. Aggressive buying (taker share)
shows only in the pump hour itself.

## 3. After the pump hour closes, there is no follow-through

Price against the close of the pump hour, what buying at its end would have seen:

| After | Pumps | Median | Median excess [95 % CI] | Up: pumps / controls |
|---|---|---|---|---|
| +1 h | 198 | +0.05 % | +0.12 % [−0.50 %; +0.35 %] | 51 % / 47 % |
| +4 h | 198 | −0.21 % | −0.18 % [−1.26 %; +0.51 %] | 47 % / 48 % |
| +24 h | 198 | −0.24 % | +0.20 % [−2.15 %; +2.09 %] | 49 % / 45 % |

Every interval contains zero, for single-coin pumps and market moves alike. The move is the
pump hour; once it is visible, the next 24 hours look like ordinary hours. An entry after
the signal has no edge even before fees and slippage.

## 4. As a predictor, the volume footprint is weak

Turning the view around: how often is an hour that looks like a pre-pump hour followed by
a pump (`mart_pump_signal_check`, 199 686 hours with a volume baseline)?

| Rule at hour t | Hours | Next hour ≥ 5 % | Precision | Lift [95 % CI] | Recall |
|---|---|---|---|---|---|
| Any hour (base rate) | 199 686 | 257 | 0.13 % | 1 | 100 % |
| Volume ≥ 2× its 7-day mean | 19 786 | 114 | 0.58 % | 4.5× [3.7; 5.2] | 44 % |
| Volume ≥ 3×, price up but < 5 % | 4 043 | 33 | 0.82 % | 6.3× [4.2; 8.7] | 13 % |

The lift is real, yet more than 99 % of the alerts are false: P(signal | pump) is high,
P(pump | signal) is not. The same trade-off as a data-quality rule with high recall and
thousands of false alarms.

## 5. Funding shows no short squeeze; positioning is a hypothesis

**Funding.** The funding premium (rate minus the contract's interest rate) is below zero
at the start of 68 % of the pumps, against 72 % of their ordinary hours: difference
−4.3 p.p. [−14.5; +4.3], median excess −0.02 bp [−0.25; +0.16] (198 pumps, 113 days).
Shorts paying longs is the norm on these coins, not a setup. This corrects the first
look in week 3, which saw a negative premium before pumps without a comparison.

**Long/short and open interest** exist for about 30 days only (API history, collected since late
October 2026): 28–30 pumps on 6–8 days, mostly three market rallies (2026-11-15, -21, -23).

| Top traders' long/short (positions) | Median excess [95 % CI] |
|---|---|
| 24 h before vs the start | +8.6 % [+1.5 %; +19.1 %] |
| 6 h before vs the start | +3.3 % [+0.0 %; +5.7 %] |

The ratio was about 8 % higher a day before the start than at the start, while ordinary
hours stayed flat: top traders leaned short into the rise. With six days behind it, and
many metrics × offsets tried, this is a **hypothesis to check as history accumulates**,
not a finding. Open interest gives no conclusion (every interval crosses zero).

## Limits

- **Survivorship.** The 23 coins were selected in 2026-11 by liquidity and a year of
  history; coins delisted during the year are not in the sample.
- **One market regime.** A single year; the futures metrics cover one month.
- **Hourly data.** A 5 % hour can be a minute-long spike; the data cannot say when inside
  the hour the move happened or whether an entry was possible at that price.
- **Many comparisons.** About 50 metric × offset comparisons are reported; a few intervals that
  exclude zero would appear by chance. The findings above rest on effects that hold over
  several neighbouring hours (volume, price) or on clear nulls.
- **Changed method.** Week 5, step 1 compared the long/short level with each coin's
  30-day median. That median included hours after the pump (look-ahead), so it was
  replaced by the change before the start.

## Where the numbers live

| Model | Grain | Role |
|---|---|---|
| `fct_pump_events` | pump | the episodes and their scope |
| `fct_pump_event_window`, `fct_pump_control_window` | anchor × hour −24…+24 | pumps and ordinary hours, one macro (`pump_window`) |
| `fct_pump_event_excess` | pump × hour × metric | value, its controls' median and shares, excess |
| `mart_pump_profile` | scope × hour × metric | medians for the dashboard's profile |
| `mart_pump_signal_check` | rule × day | hours and next-hour pumps per rule |

All of them are exported to Parquet (`data/marts/`) after each pipeline run; the dashboard
and `analysis/pump_findings.py` read the export, not the warehouse.
