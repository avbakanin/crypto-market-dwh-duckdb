-- Marts metrics of the DuckDB warehouse, as the marts branches of
-- docs/baseline/snowflake_baseline.sql compute them in Snowflake: same relations, metric
-- names and expressions (count(distinct a, b) -> count(distinct (a, b)),
-- to_date(ts) -> ts::date in the UTC session, number(38, s) -> decimal(38, s)).
-- One row per metric, every value as text. table_hash (hash_agg) has no DuckDB
-- counterpart: the baseline marks it snowflake_only. Metrics of the control hours are
-- engine_dependent (md5 picks other hours than Snowflake's hash): computed here so the
-- report can show them side by side. Sums of DOUBLE columns use fsum (compensated
-- summation, source note 112); sums of decimals stay exact sum().
-- Run by analysis/compare_baseline.py --layer marts.

select 'marts' as layer, 'dim_symbol' as relation, 'all' as grain, metric, value
from (
    select
        count(*)::varchar as "rows",
        count_if(has_perpetual)::varchar as has_perpetual,
        sum(contract_multiplier)::varchar as sum_contract_multiplier,
        sum(interest_rate_8h)::varchar as sum_interest_rate_8h
    from marts.dim_symbol
) unpivot (value for metric in ("rows", has_perpetual, sum_contract_multiplier, sum_interest_rate_8h))

union all

select 'marts', 'fct_pump_events', 'all', metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct symbol)::varchar as symbols,
        count(distinct event_hour::date)::varchar as days,
        min(event_open_time_ms)::varchar as min_event_open_time_ms,
        max(event_open_time_ms)::varchar as max_event_open_time_ms,
        sum(event_open_time_ms)::varchar as sum_event_open_time_ms,
        min(pump_event_id) as min_pump_event_id,
        max(pump_event_id) as max_pump_event_id,
        sum(episode_hours)::varchar as sum_episode_hours,
        sum(market_breadth)::varchar as sum_market_breadth,
        count_if(overlaps_previous_event)::varchar as overlaps_previous_event,
        count_if(has_full_before_window)::varchar as has_full_before_window,
        count_if(has_full_after_window)::varchar as has_full_after_window,
        count_if(has_full_open_interest_window)::varchar as has_full_open_interest_window,
        round(sum(return_1h), 9)::decimal(38, 9)::varchar as sum_return_1h,
        count(return_before)::varchar as count_return_before,
        round(fsum(return_before), 9)::decimal(38, 9)::varchar as sum_return_before,
        count(return_after)::varchar as count_return_after,
        round(fsum(return_after), 9)::decimal(38, 9)::varchar as sum_return_after,
        count(volume_ratio_event)::varchar as count_volume_ratio_event,
        round(fsum(volume_ratio_event), 6)::decimal(38, 6)::varchar as sum_volume_ratio_event,
        count(open_interest_change_event)::varchar as count_open_interest_change_event,
        round(fsum(open_interest_change_event), 9)::decimal(38, 9)::varchar
            as sum_open_interest_change_event,
        count(funding_rate_8h_before)::varchar as count_funding_rate_8h_before,
        round(sum(funding_rate_8h_before), 12)::decimal(38, 12)::varchar
            as sum_funding_rate_8h_before
    from marts.fct_pump_events
) unpivot (value for metric in ("rows", symbols, days, min_event_open_time_ms, max_event_open_time_ms, sum_event_open_time_ms, min_pump_event_id, max_pump_event_id, sum_episode_hours, sum_market_breadth, overlaps_previous_event, has_full_before_window, has_full_after_window, has_full_open_interest_window, sum_return_1h, count_return_before, sum_return_before, count_return_after, sum_return_after, count_volume_ratio_event, sum_volume_ratio_event, count_open_interest_change_event, sum_open_interest_change_event, count_funding_rate_8h_before, sum_funding_rate_8h_before))

union all

select 'marts', 'fct_pump_events', event_scope, 'rows', count(*)::varchar
from marts.fct_pump_events
group by event_scope

union all

select 'marts', 'fct_pump_events', grain, metric, value
from (
    select
        symbol as grain,
        count(*)::varchar as "rows",
        sum(event_open_time_ms)::varchar as sum_event_open_time_ms
    from marts.fct_pump_events
    group by symbol
) unpivot (value for metric in ("rows", sum_event_open_time_ms))

union all

select 'marts', 'fct_pump_event_window', 'all', metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct pump_event_id)::varchar as events,
        count(return_since_anchor_close)::varchar as count_return_since_anchor_close,
        round(fsum(return_since_anchor_close), 9)::decimal(38, 9)::varchar
            as sum_return_since_anchor_close,
        count(volume_ratio)::varchar as count_volume_ratio,
        round(fsum(volume_ratio), 6)::decimal(38, 6)::varchar as sum_volume_ratio,
        count(taker_buy_share)::varchar as count_taker_buy_share,
        round(fsum(taker_buy_share), 9)::decimal(38, 9)::varchar as sum_taker_buy_share,
        count(open_interest_change_since_start)::varchar
            as count_open_interest_change_since_start,
        count(top_position_ratio_change_since_start)::varchar
            as count_top_position_ratio_change,
        round(fsum(top_position_ratio_change_since_start), 9)::decimal(38, 9)::varchar
            as sum_top_position_ratio_change,
        count_if(is_funding_stale)::varchar as funding_stale_rows
    from marts.fct_pump_event_window
) unpivot (value for metric in ("rows", events, count_return_since_anchor_close, sum_return_since_anchor_close, count_volume_ratio, sum_volume_ratio, count_taker_buy_share, sum_taker_buy_share, count_open_interest_change_since_start, count_top_position_ratio_change, sum_top_position_ratio_change, funding_stale_rows))

union all

select 'marts', 'fct_pump_control_window', 'all', metric, value
from (
    select
        count(*)::varchar as "rows",
        count(distinct pump_event_id)::varchar as events,
        count_if(is_year_control)::varchar as year_control_rows,
        count_if(is_recent_control)::varchar as recent_control_rows,
        count(distinct (pump_event_id, control_open_time_ms))::varchar as distinct_control_hours
    from marts.fct_pump_control_window
) unpivot (value for metric in ("rows", events, year_control_rows, recent_control_rows, distinct_control_hours))

union all

select 'marts', 'fct_pump_event_excess', 'all', 'rows', count(*)::varchar
from marts.fct_pump_event_excess

union all

select 'marts', 'fct_pump_event_excess', grain, metric, value
from (
    select
        metric as grain,
        count(*)::varchar as "rows",
        count(value)::varchar as count_value,
        round(fsum(value), 9)::decimal(38, 9)::varchar as sum_value,
        sum(control_hours)::varchar as sum_control_hours,
        round(fsum(excess), 9)::decimal(38, 9)::varchar as sum_excess
    from marts.fct_pump_event_excess
    group by metric
) unpivot (value for metric in ("rows", count_value, sum_value, sum_control_hours, sum_excess))

union all

select 'marts', 'mart_pump_profile', 'all', 'rows', count(*)::varchar
from marts.mart_pump_profile

union all

select 'marts', 'mart_pump_profile', grain, metric, value
from (
    select
        metric as grain,
        count(*)::varchar as "rows",
        sum(events)::varchar as sum_events,
        round(fsum(event_median), 9)::decimal(38, 9)::varchar as sum_event_median,
        round(fsum(control_median), 9)::decimal(38, 9)::varchar as sum_control_median,
        round(fsum(median_excess), 9)::decimal(38, 9)::varchar as sum_median_excess
    from marts.mart_pump_profile
    group by metric
) unpivot (value for metric in ("rows", sum_events, sum_event_median, sum_control_median, sum_median_excess))

union all

select 'marts', 'mart_pump_signal_check', 'all', 'rows', count(*)::varchar
from marts.mart_pump_signal_check

union all

select 'marts', 'mart_pump_signal_check', grain, metric, value
from (
    select
        rule as grain,
        count(*)::varchar as days,
        sum(signal_hours)::varchar as sum_signal_hours,
        sum(followed_by_pump)::varchar as sum_followed_by_pump
    from marts.mart_pump_signal_check
    group by rule
) unpivot (value for metric in (days, sum_signal_hours, sum_followed_by_pump))
