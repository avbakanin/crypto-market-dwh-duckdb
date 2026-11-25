-- Intermediate metrics of the DuckDB warehouse, as the intermediate branches of
-- docs/baseline/snowflake_baseline.sql compute them in Snowflake: same relations, metric
-- names and expressions (count(distinct a, b) -> count(distinct (a, b)),
-- number(38, s) -> decimal(38, s)). One row per metric, every value as text.
-- table_hash (hash_agg) has no DuckDB counterpart: the baseline marks it snowflake_only.
-- Sums of DOUBLE columns use fsum (compensated summation): plain sum() of ~200k doubles
-- drifted by 7e-3 on a 1.2e12 total (source note 112); sums of decimals stay exact sum().
-- Run by analysis/compare_baseline.py --layer intermediate.

select 'intermediate' as layer, 'int_binance__hourly_returns' as relation, 'all' as grain, metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct (symbol, open_time_ms))::varchar as distinct_keys,
        count(return_1h)::varchar as count_return_1h,
        round(sum(return_1h), 9)::decimal(38, 9)::varchar as sum_return_1h,
        count_if(return_1h >= 0.05)::varchar as pump_hours_5pct,
        count(return_zscore)::varchar as count_return_zscore,
        round(fsum(return_zscore), 6)::decimal(38, 6)::varchar as sum_return_zscore,
        count(quote_volume_baseline)::varchar as count_quote_volume_baseline,
        round(fsum(quote_volume_baseline), 4)::decimal(38, 4)::varchar as sum_quote_volume_baseline,
        count(log_volume_zscore)::varchar as count_log_volume_zscore,
        round(fsum(log_volume_zscore), 6)::decimal(38, 6)::varchar as sum_log_volume_zscore,
        count_if(log_volume_zscore > 5)::varchar as log_volume_zscore_above_5,
        count(taker_buy_share)::varchar as count_taker_buy_share,
        round(fsum(taker_buy_share), 9)::decimal(38, 9)::varchar as sum_taker_buy_share
    from staging.int_binance__hourly_returns
) unpivot (value for metric in ("rows", distinct_keys, count_return_1h, sum_return_1h, pump_hours_5pct, count_return_zscore, sum_return_zscore, count_quote_volume_baseline, sum_quote_volume_baseline, count_log_volume_zscore, sum_log_volume_zscore, log_volume_zscore_above_5, count_taker_buy_share, sum_taker_buy_share))

union all

select 'intermediate', 'int_binance__hourly_returns', grain, metric, value
from (
    select
        symbol as grain,
        count(*)::varchar as "rows",
        count(return_1h)::varchar as count_return_1h,
        count_if(return_1h >= 0.05)::varchar as pump_hours_5pct,
        count(return_zscore)::varchar as count_return_zscore
    from staging.int_binance__hourly_returns
    group by symbol
) unpivot (value for metric in ("rows", count_return_1h, pump_hours_5pct, count_return_zscore))

union all

select 'intermediate', 'int_binance__hourly_market', 'all', metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct (symbol, open_time_ms))::varchar as distinct_keys,
        count(open_interest_coins)::varchar as count_open_interest_coins,
        sum(open_interest_coins)::varchar as sum_open_interest_coins,
        count(funding_rate)::varchar as count_funding_rate,
        sum(funding_rate)::varchar as sum_funding_rate,
        round(sum(funding_rate_8h), 12)::decimal(38, 12)::varchar as sum_funding_rate_8h,
        round(sum(funding_premium_8h), 12)::decimal(38, 12)::varchar as sum_funding_premium_8h,
        count_if(is_funding_stale)::varchar as funding_stale_hours,
        count(top_account_long_short_ratio)::varchar as count_top_account_ratio,
        sum(top_account_long_short_ratio)::varchar as sum_top_account_ratio,
        count(top_position_long_short_ratio)::varchar as count_top_position_ratio,
        sum(top_position_long_short_ratio)::varchar as sum_top_position_ratio,
        count(global_account_long_short_ratio)::varchar as count_global_account_ratio,
        sum(global_account_long_short_ratio)::varchar as sum_global_account_ratio
    from staging.int_binance__hourly_market
) unpivot (value for metric in ("rows", distinct_keys, count_open_interest_coins, sum_open_interest_coins, count_funding_rate, sum_funding_rate, sum_funding_rate_8h, sum_funding_premium_8h, funding_stale_hours, count_top_account_ratio, sum_top_account_ratio, count_top_position_ratio, sum_top_position_ratio, count_global_account_ratio, sum_global_account_ratio))

union all

select 'intermediate', 'int_binance__hourly_market', grain, metric, value
from (
    select
        symbol as grain,
        count(*)::varchar as "rows",
        count(open_interest_coins)::varchar as count_open_interest_coins,
        count(funding_rate)::varchar as count_funding_rate,
        count(top_position_long_short_ratio)::varchar as count_top_position_ratio,
        count_if(is_funding_stale)::varchar as funding_stale_hours
    from staging.int_binance__hourly_market
    group by symbol
) unpivot (value for metric in ("rows", count_open_interest_coins, count_funding_rate, count_top_position_ratio, funding_stale_hours))
