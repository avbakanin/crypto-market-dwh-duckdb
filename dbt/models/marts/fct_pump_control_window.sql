-- Ordinary hours to compare each pump with: per event, control hours of the same symbol
-- measured exactly like the event window (macro pump_window). Same symbol, because a few
-- coins have most of the events and their normal state is not the market's.
-- An hour is a candidate when its whole window is inside the loaded history, it has a
-- volume baseline, and no hour of the symbol rose past the threshold within
-- 2 * pump_window_hours rows on either side: neither its own window nor the "after" of a
-- pump reaches it.
-- Two pools of control_hours_per_event each, since history depth differs:
--   year:   any candidate - for price, volume, taker share and funding (a year).
--   recent: candidates with open interest and long/short ratios in every hour of the
--           window (~30 days) - for those metrics, which a year-wide pick would rarely hit.
-- The pick orders by md5 of (event, hour), not random(): the same hours on every rebuild,
-- so the profile changes only when the data does. md5, not hash(): hash() differs between
-- engines and is not promised stable across DuckDB versions; md5 is the same everywhere.

{%- set window = var('pump_window_hours') %}
{%- set threshold = var('pump_min_return') %}
{%- set per_event = var('control_hours_per_event') %}

with events as (

    select
        pump_event_id,
        symbol
    from {{ ref('fct_pump_events') }}

),

hours as (

    select
        symbol,
        open_time_ms,
        quote_volume_baseline,
        lag(open_time_ms, {{ window + 1 }}) over (partition by symbol order by open_time_ms)
            as window_start_ms,
        lead(open_time_ms, {{ window }}) over (partition by symbol order by open_time_ms)
            as window_end_ms,
        count_if(return_1h >= {{ threshold }}) over (
            partition by symbol order by open_time_ms
            rows between {{ 2 * window }} preceding and {{ 2 * window }} following
        ) as qualifying_hours_nearby,
        count(open_interest_coins) over (
            partition by symbol order by open_time_ms
            rows between {{ window + 1 }} preceding and {{ window }} following
        ) as open_interest_hours,
        count(top_position_long_short_ratio) over (
            partition by symbol order by open_time_ms
            rows between {{ window + 1 }} preceding and {{ window }} following
        ) as long_short_hours
    from {{ ref('int_binance__hourly_market') }}

),

candidates as (

    select
        symbol,
        open_time_ms,
        open_interest_hours = {{ 2 * window + 2 }}
        and long_short_hours = {{ 2 * window + 2 }} as has_futures_window
    from hours
    where
        window_start_ms = open_time_ms - {{ window + 1 }} * 3600000
        and window_end_ms = open_time_ms + {{ window }} * 3600000
        and quote_volume_baseline is not null
        and qualifying_hours_nearby = 0

),

ranked as (

    select
        events.pump_event_id,
        -- The candidate's symbol, not the event's: equal by the join, and a broken join
        -- shows up as foreign rows in assert_pump_control_window_is_complete.
        candidates.symbol,
        candidates.open_time_ms as control_open_time_ms,
        row_number() over (
            partition by events.pump_event_id
            order by
                md5(events.pump_event_id || '-' || candidates.open_time_ms),
                candidates.open_time_ms
        ) as year_rank,
        if(
            candidates.has_futures_window,
            row_number() over (
                partition by events.pump_event_id, candidates.has_futures_window
                order by
                    md5(events.pump_event_id || '-' || candidates.open_time_ms),
                    candidates.open_time_ms
            ),
            null
        ) as recent_rank
    from events
    inner join candidates
        on events.symbol = candidates.symbol

),

anchors as (

    select
        pump_event_id,
        symbol,
        control_open_time_ms,
        year_rank <= {{ per_event }} as is_year_control,
        coalesce(recent_rank <= {{ per_event }}, false) as is_recent_control,
        control_open_time_ms as anchor_open_time_ms
    from ranked
    where year_rank <= {{ per_event }} or recent_rank <= {{ per_event }}

)

{{ pump_window('anchors') }}
