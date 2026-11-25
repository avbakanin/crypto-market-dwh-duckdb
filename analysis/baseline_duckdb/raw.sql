-- RAW metrics of the DuckDB warehouse, as the raw branches of
-- docs/baseline/snowflake_baseline.sql compute them in Snowflake: same relations, metric
-- names and expressions (payload[0] -> payload->>0, payload:x -> payload->>'x',
-- number(38, 8) -> decimal(38, 8)). One row per metric, every value as text.
-- content_hash (hash_agg) has no DuckDB counterpart: the baseline marks it snowflake_only.
-- Run by analysis/compare_baseline.py --layer raw.

select 'raw' as layer, 'binance_spot_klines_1h' as relation, 'all' as grain, metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct symbol)::varchar as symbols,
        count(distinct _batch_id)::varchar as batches,
        min(_batch_id) as first_batch_id,
        max(_batch_id) as last_batch_id,
        count(distinct (symbol, (payload->>0)::bigint))::varchar as distinct_keys,
        min((payload->>0)::bigint)::varchar as min_key_ms,
        max((payload->>0)::bigint)::varchar as max_key_ms,
        sum((payload->>0)::bigint)::varchar as sum_key_ms,
        sum((payload->>4)::decimal(38, 8))::varchar as sum_close
    from raw.binance_spot_klines_1h
) unpivot (value for metric in ("rows", symbols, batches, first_batch_id, last_batch_id, distinct_keys, min_key_ms, max_key_ms, sum_key_ms, sum_close))

union all

select 'raw' as layer, 'binance_futures_funding_rate' as relation, 'all' as grain, metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct symbol)::varchar as symbols,
        count(distinct _batch_id)::varchar as batches,
        min(_batch_id) as first_batch_id,
        max(_batch_id) as last_batch_id,
        count(distinct (symbol, (payload->>'fundingTime')::bigint // 3600000 * 3600000))::varchar as distinct_keys,
        min((payload->>'fundingTime')::bigint // 3600000 * 3600000)::varchar as min_key_ms,
        max((payload->>'fundingTime')::bigint // 3600000 * 3600000)::varchar as max_key_ms,
        sum((payload->>'fundingTime')::bigint // 3600000 * 3600000)::varchar as sum_key_ms,
        sum((payload->>'fundingRate')::decimal(38, 8))::varchar as sum_funding_rate
    from raw.binance_futures_funding_rate
) unpivot (value for metric in ("rows", symbols, batches, first_batch_id, last_batch_id, distinct_keys, min_key_ms, max_key_ms, sum_key_ms, sum_funding_rate))

union all

select 'raw' as layer, 'binance_futures_open_interest_1h' as relation, 'all' as grain, metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct symbol)::varchar as symbols,
        count(distinct _batch_id)::varchar as batches,
        min(_batch_id) as first_batch_id,
        max(_batch_id) as last_batch_id,
        count(distinct (symbol, (payload->>'timestamp')::bigint))::varchar as distinct_keys,
        min((payload->>'timestamp')::bigint)::varchar as min_key_ms,
        max((payload->>'timestamp')::bigint)::varchar as max_key_ms,
        sum((payload->>'timestamp')::bigint)::varchar as sum_key_ms,
        sum((payload->>'sumOpenInterest')::decimal(38, 8))::varchar as sum_open_interest
    from raw.binance_futures_open_interest_1h
) unpivot (value for metric in ("rows", symbols, batches, first_batch_id, last_batch_id, distinct_keys, min_key_ms, max_key_ms, sum_key_ms, sum_open_interest))

union all

select 'raw' as layer, 'binance_futures_top_account_ratio_1h' as relation, 'all' as grain, metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct symbol)::varchar as symbols,
        count(distinct _batch_id)::varchar as batches,
        min(_batch_id) as first_batch_id,
        max(_batch_id) as last_batch_id,
        count(distinct (symbol, (payload->>'timestamp')::bigint))::varchar as distinct_keys,
        min((payload->>'timestamp')::bigint)::varchar as min_key_ms,
        max((payload->>'timestamp')::bigint)::varchar as max_key_ms,
        sum((payload->>'timestamp')::bigint)::varchar as sum_key_ms,
        sum((payload->>'longShortRatio')::decimal(38, 8))::varchar as sum_long_short_ratio
    from raw.binance_futures_top_account_ratio_1h
) unpivot (value for metric in ("rows", symbols, batches, first_batch_id, last_batch_id, distinct_keys, min_key_ms, max_key_ms, sum_key_ms, sum_long_short_ratio))

union all

select 'raw' as layer, 'binance_futures_top_position_ratio_1h' as relation, 'all' as grain, metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct symbol)::varchar as symbols,
        count(distinct _batch_id)::varchar as batches,
        min(_batch_id) as first_batch_id,
        max(_batch_id) as last_batch_id,
        count(distinct (symbol, (payload->>'timestamp')::bigint))::varchar as distinct_keys,
        min((payload->>'timestamp')::bigint)::varchar as min_key_ms,
        max((payload->>'timestamp')::bigint)::varchar as max_key_ms,
        sum((payload->>'timestamp')::bigint)::varchar as sum_key_ms,
        sum((payload->>'longShortRatio')::decimal(38, 8))::varchar as sum_long_short_ratio
    from raw.binance_futures_top_position_ratio_1h
) unpivot (value for metric in ("rows", symbols, batches, first_batch_id, last_batch_id, distinct_keys, min_key_ms, max_key_ms, sum_key_ms, sum_long_short_ratio))

union all

select 'raw' as layer, 'binance_futures_global_account_ratio_1h' as relation, 'all' as grain, metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct symbol)::varchar as symbols,
        count(distinct _batch_id)::varchar as batches,
        min(_batch_id) as first_batch_id,
        max(_batch_id) as last_batch_id,
        count(distinct (symbol, (payload->>'timestamp')::bigint))::varchar as distinct_keys,
        min((payload->>'timestamp')::bigint)::varchar as min_key_ms,
        max((payload->>'timestamp')::bigint)::varchar as max_key_ms,
        sum((payload->>'timestamp')::bigint)::varchar as sum_key_ms,
        sum((payload->>'longShortRatio')::decimal(38, 8))::varchar as sum_long_short_ratio
    from raw.binance_futures_global_account_ratio_1h
) unpivot (value for metric in ("rows", symbols, batches, first_batch_id, last_batch_id, distinct_keys, min_key_ms, max_key_ms, sum_key_ms, sum_long_short_ratio))
