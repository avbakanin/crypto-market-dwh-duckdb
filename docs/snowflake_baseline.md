# Snowflake baseline

A fingerprint of every layer of the Snowflake version, taken before the trial account
ends (about 2026-12-20). The project moves to DuckDB next. This file is what the copy
proves itself against: the same landing zone batches in, the same numbers out.

## What was captured

| | |
|---|---|
| State | right after the scheduled run of 2026-11-25 09:07 UTC; the DAG was paused at 11:01 UTC, so nothing changed while the queries ran |
| Input cutoff | batch `20261125T090706Z`: the newest batch of all six RAW datasets |
| Query | [baseline/snowflake_baseline.sql](baseline/snowflake_baseline.sql): read only, on `COMPUTE_WH`, one row per metric |
| Result | [baseline/snowflake_baseline_metrics.csv](baseline/snowflake_baseline_metrics.csv): 758 metrics, captured 11:22 UTC |
| Row-level copy | the mart export of the same run, `data/marts/20261125T091007Z`, pinned as `data/baseline/marts_20261125T091007Z` (8 Parquet files, 72 607 rows; not in git, SHA-256 below) |

Each metric is one CSV row: `layer, relation, grain, metric, compare, value`. The grain is
`all` or a split: per symbol for candles, returns, market and events; per series, scope,
metric, rule or dataset elsewhere.

## Rows per layer

| Layer | Relation | Rows |
|---|---|---:|
| RAW | `binance_spot_klines_1h` | 204 838 (203 987 keys, 38 batches) |
| RAW | `binance_futures_funding_rate` | 30 205 (29 885 keys) |
| RAW | `binance_futures_open_interest_1h` | 19 550 (18 768 keys) |
| RAW | three long/short ratio tables | 18 492 each (17 986 keys) |
| staging | `stg_binance__klines_1h` | 203 987 = 23 symbols × 8 869 hours |
| staging | `stg_binance__funding_rate` / `_open_interest_1h` / `_long_short_ratio_1h` | 29 885 / 18 768 / 53 958 |
| intermediate | `int_binance__hourly_returns`, `int_binance__hourly_market` | 203 987 each |
| marts | `fct_pump_events` | 198 events, 21 symbols, 113 days (120 single coin, 78 multi coin) |
| marts | `fct_pump_event_window` / `fct_pump_event_excess` / `mart_pump_profile` | 9 702 / 53 837 / 1 323 |
| marts | `fct_pump_control_window` / `mart_pump_signal_check` / `dim_symbol` | 559 335 / 1 782 / 23 |
| DQ | last build | 176 checks: 174 pass, 2 warn (`funding_missing_settlements` 4, `volume_anomaly` 24); `dq_log` reconciliation pass |

RAW has more rows than keys on purpose: it is a journal, and overlapping batches keep every
version. Staging keeps the newest version of each key.

## How to compare a metric

| `compare` | Metrics | Rule |
|---|---:|---|
| `exact` | 641 | must be equal. Counts, key bounds and sums, sums of source decimals (prices, rates, open interest), batch ids, event ids (md5), DQ findings |
| `tolerance` | 36 | sums of derived values (returns, z-scores, ratios), rounded to the printed scale. Snowflake divides decimals exactly and DuckDB in double, so allow a few units in the last printed digit |
| `engine_dependent` | 38 | the control sample is picked by `hash()`, and DuckDB's `hash()` differs: other hours, same size per event. Compare the distribution, not the value |
| `reference` | 14 | check counts per type and status. The copy's set of checks may differ (the DQ log reconciliation reads Snowflake's `INFORMATION_SCHEMA`) |
| `snowflake_only` | 29 | `hash_agg` of each table and the DQ journal's history. Only a Snowflake rerun can reproduce them |

## Two checks of the baseline itself

**The landing zone rebuilds RAW.** `python -m analysis.landing_zone_fingerprint --until
20261125T090706Z --compare docs/baseline/snowflake_baseline_metrics.csv` computes the raw
metrics from `data/raw` alone: complete batches, the files their manifests list. Result:
all 60 exact raw metrics equal. So the local batches are a complete copy of RAW, and any
engine that loads them starts from the same input.

**The models are deterministic.** A second `dbt build` on the unchanged RAW (11:28 UTC,
`PASS=193 WARN=2`), then the same query again: 754 of 758 metrics equal, and all 21
`hash_agg` fingerprints equal. The four differences are expected: the capture time and three
journal metrics (+2 invocations, +194 rows). Equal fingerprints after a full rebuild mean
no model depends on run time or on the order of ties. Without that, comparing two engines
would not be meaningful.

## How the DuckDB copy proves itself

1. Load only batches up to `20261125T090706Z` into RAW. Run the landing zone check: 60 of 60.
2. Build the models. Port the baseline query; most of it is aggregates that DuckDB has.
   Replace `hash_agg`, write `count(distinct (a, b))` for multi-column distinct counts,
   and use `epoch_ms` for times.
3. Compare per the `compare` column. Start with `exact`: a difference there points to one
   relation and, through the per-symbol grain, often to one symbol. When a metric differs,
   recompute it exactly before deciding which side is wrong: the baseline can be the one
   that is off (`sum_return_zscore`, source note 113). `python -m analysis.exact_reference`
   writes `docs/baseline/exact_references.csv`; a metric counts as a *baseline error* only
   when DuckDB is within tolerance of the exact value and the baseline is not.
4. Row level: compare the marts with the pinned export. Rows are joined on each table's key
   in both directions (rows only on one side are the `EXCEPT` of keys) and every column is
   compared, numbers within 1e-9 relative: a plain `EXCEPT` of whole rows would fail on
   every DOUBLE that Snowflake stored as a decimal. The columns of the control hours are
   compared as distributions (medians, two-sample Kolmogorov-Smirnov by events).

The commands: `DUCKDB_PATH=warehouse/baseline_20261125T090706Z.duckdb` for each of
`python -m ingestion.load_raw --until 20261125T090706Z`,
`python -m analysis.compare_baseline --layer raw [--layer ...] --report
docs/duckdb_vs_snowflake.md` and `python -m analysis.compare_export --export
<original>/data/baseline/marts_20261125T091007Z --report docs/duckdb_vs_snowflake_export.md`.
The DuckDB side of each layer is `analysis/baseline_duckdb/<layer>.sql`; the results are
[docs/duckdb_vs_snowflake.md](duckdb_vs_snowflake.md) and
[docs/duckdb_vs_snowflake_export.md](duckdb_vs_snowflake_export.md).

The batches up to the cutoff must outlive the trial. They are immutable, so backing up
`data/raw` once is enough.

## Pinned export (SHA-256)

```
12427769955de9f54f374e7baf273f7a644829d8334d5d7b5e4ce318a8615580  _manifest.json
9e58b891414714e7407a12fcff841ba35a1bda7e0b7ac49560af682811fd48e4  dim_symbol.parquet
55c9d057ca48f123659b354126a4cf7b75ffa58bcd4e32cdb5b0082d21af1a45  dq_check_results.parquet
9bf53a6ff1becbd15dd2432856f88d3074dc07cd40e5225fe3f71512d8991d6a  dq_data_coverage.parquet
e7131d8ee2995e255e7f752624b9c2c958ee491782d388ea7eb41f2fef4ef8ff  fct_pump_event_excess.parquet
4f6608efade19107c4b2e8710f704e318db1bfa8a53cf0ddced520feb80a6646  fct_pump_event_window.parquet
e5f9d9e78f24c458e45b7ad7fb3b31ea8f5ed91cf3fbf2308e2e886ff511c8f7  fct_pump_events.parquet
721319764d18d9a4c64f0d7c235c90022a185317c6d9b0f012fdd32e9f2f7f7a  mart_pump_profile.parquet
743d96c7a08a0a8dcb4e066996f8044c9ae4b111bf8fb88853f5094e7028cdd6  mart_pump_signal_check.parquet
```
