-- Staging metrics of the DuckDB warehouse, as the staging branches of
-- docs/baseline/snowflake_baseline.sql compute them in Snowflake: same relations, metric
-- names and expressions (count(distinct a, b) -> count(distinct (a, b)),
-- number(38, 6) -> decimal(38, 6)). One row per metric, every value as text.
-- table_hash (hash_agg) has no DuckDB counterpart: the baseline marks it snowflake_only.
-- Run by analysis/compare_baseline.py --layer staging.

select 'staging' as layer, 'stg_binance__klines_1h' as relation, 'all' as grain, metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct symbol)::varchar as symbols,
        count(distinct (symbol, open_time_ms))::varchar as distinct_keys,
        min(open_time_ms)::varchar as min_open_time_ms,
        max(open_time_ms)::varchar as max_open_time_ms,
        sum(open_time_ms)::varchar as sum_open_time_ms,
        sum(close_price)::varchar as sum_close_price,
        sum(base_volume)::varchar as sum_base_volume,
        sum(quote_volume)::varchar as sum_quote_volume,
        sum(_version_count)::varchar as sum_version_count,
        count_if(_is_revised)::varchar as revised_keys,
        max(_batch_id) as last_batch_id
    from staging.stg_binance__klines_1h
) unpivot (value for metric in ("rows", symbols, distinct_keys, min_open_time_ms, max_open_time_ms, sum_open_time_ms, sum_close_price, sum_base_volume, sum_quote_volume, sum_version_count, revised_keys, last_batch_id))

union all

select 'staging', 'stg_binance__klines_1h', grain, metric, value
from (
    select
        symbol as grain,
        count(*)::varchar as "rows",
        min(open_time_ms)::varchar as min_open_time_ms,
        max(open_time_ms)::varchar as max_open_time_ms,
        sum(close_price)::varchar as sum_close_price,
        sum(quote_volume)::varchar as sum_quote_volume
    from staging.stg_binance__klines_1h
    group by symbol
) unpivot (value for metric in ("rows", min_open_time_ms, max_open_time_ms, sum_close_price, sum_quote_volume))

union all

select 'staging', 'stg_binance__funding_rate', 'all', metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct contract_symbol)::varchar as contracts,
        count(distinct (contract_symbol, funding_hour_ms))::varchar as distinct_keys,
        min(funding_hour_ms)::varchar as min_funding_hour_ms,
        max(funding_hour_ms)::varchar as max_funding_hour_ms,
        sum(funding_rate)::varchar as sum_funding_rate,
        count(mark_price)::varchar as count_mark_price,
        sum(mark_price)::varchar as sum_mark_price,
        round(sum(mark_price_per_coin), 6)::decimal(38, 6)::varchar as sum_mark_price_per_coin,
        sum(interval_hours)::varchar as sum_interval_hours,
        count_if(is_after_missed_settlement)::varchar as after_missed_settlement,
        count_if(_is_revised)::varchar as revised_keys
    from staging.stg_binance__funding_rate
) unpivot (value for metric in ("rows", contracts, distinct_keys, min_funding_hour_ms, max_funding_hour_ms, sum_funding_rate, count_mark_price, sum_mark_price, sum_mark_price_per_coin, sum_interval_hours, after_missed_settlement, revised_keys))

union all

select 'staging', 'stg_binance__open_interest_1h', 'all', metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct contract_symbol)::varchar as contracts,
        count(distinct (contract_symbol, snapshot_time_ms))::varchar as distinct_keys,
        min(snapshot_time_ms)::varchar as min_snapshot_time_ms,
        max(snapshot_time_ms)::varchar as max_snapshot_time_ms,
        sum(open_interest)::varchar as sum_open_interest,
        sum(open_interest_coins)::varchar as sum_open_interest_coins,
        sum(open_interest_usdt)::varchar as sum_open_interest_usdt,
        count_if(_is_revised)::varchar as revised_keys
    from staging.stg_binance__open_interest_1h
) unpivot (value for metric in ("rows", contracts, distinct_keys, min_snapshot_time_ms, max_snapshot_time_ms, sum_open_interest, sum_open_interest_coins, sum_open_interest_usdt, revised_keys))

union all

select 'staging', 'stg_binance__long_short_ratio_1h', 'all', metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct (contract_symbol, ratio_series, snapshot_time_ms))::varchar as distinct_keys,
        count_if(_is_revised)::varchar as revised_keys
    from staging.stg_binance__long_short_ratio_1h
) unpivot (value for metric in ("rows", distinct_keys, revised_keys))

union all

select 'staging', 'stg_binance__long_short_ratio_1h', grain, metric, value
from (
    select
        ratio_series as grain,
        count(*)::varchar as "rows",
        count(distinct contract_symbol)::varchar as contracts,
        min(snapshot_time_ms)::varchar as min_snapshot_time_ms,
        max(snapshot_time_ms)::varchar as max_snapshot_time_ms,
        sum(long_share)::varchar as sum_long_share,
        sum(short_share)::varchar as sum_short_share,
        sum(long_short_ratio)::varchar as sum_long_short_ratio
    from staging.stg_binance__long_short_ratio_1h
    group by ratio_series
) unpivot (value for metric in ("rows", contracts, min_snapshot_time_ms, max_snapshot_time_ms, sum_long_share, sum_short_share, sum_long_short_ratio))

union all

select 'staging', 'seed symbols', 'all', 'rows', count(*)::varchar from staging.symbols

union all

select 'staging', 'seed perpetuals', 'all', 'rows', count(*)::varchar from staging.perpetuals
