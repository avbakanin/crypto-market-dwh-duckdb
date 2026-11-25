-- One row per pump episode: a 1h close-to-close rise of at least pump_min_return.
-- Neighbouring qualifying hours of one move form one episode (gaps and islands): a rise
-- within pump_window_hours of the previous qualifying hour extends the episode, so no
-- other qualifying hour of the symbol sits inside an event's "before" window.
-- The event hour is the episode's first qualifying hour. Around it, row k of the market is
-- the state at the end of hour k: the event starts at the end of hour -1.
-- Edges of history are kept and flagged: metrics without a full window are NULL.

{%- set threshold = var('pump_min_return') %}
{%- set window = var('pump_window_hours') %}

with market as (

    select * from {{ ref('int_binance__hourly_market') }}

),

around as (

    select
        *,
        -- How many symbols rose past the threshold in the same hour: a market-wide move
        -- (2026-11-15 02:00: nine symbols at once) is not a pump of one coin.
        -- bigint: count_if over a window is a HUGEINT in DuckDB.
        (count_if(return_1h >= {{ threshold }}) over (partition by open_time_ms))::bigint
            as market_breadth,
        lag(open_time_ms, {{ window + 1 }}) over (partition by symbol order by open_time_ms)
            as window_start_ms,
        lead(open_time_ms, {{ window }}) over (partition by symbol order by open_time_ms)
            as window_end_ms,
        lag(close_price) over (partition by symbol order by open_time_ms)
            as close_price_before,
        lag(close_price, {{ window + 1 }}) over (partition by symbol order by open_time_ms)
            as close_price_window_start,
        lead(close_price, {{ window }}) over (partition by symbol order by open_time_ms)
            as close_price_window_end,
        avg(quote_volume) over (
            partition by symbol order by open_time_ms
            rows between {{ window }} preceding and 1 preceding
        ) as quote_volume_avg_before,
        avg(quote_volume) over (
            partition by symbol order by open_time_ms
            rows between 1 following and {{ window }} following
        ) as quote_volume_avg_after,
        lag(open_interest_coins) over (partition by symbol order by open_time_ms)
            as open_interest_coins_before,
        lag(open_interest_coins, {{ window + 1 }}) over (
            partition by symbol order by open_time_ms
        ) as open_interest_coins_window_start,
        lead(open_interest_coins, {{ window }}) over (
            partition by symbol order by open_time_ms
        ) as open_interest_coins_window_end,
        count(open_interest_coins) over (
            partition by symbol order by open_time_ms
            rows between {{ window + 1 }} preceding and {{ window }} following
        ) as open_interest_hours_in_window,
        lag(funding_rate_8h) over (partition by symbol order by open_time_ms)
            as funding_rate_8h_before,
        avg(funding_rate_8h) over (
            partition by symbol order by open_time_ms
            rows between {{ window }} preceding and 1 preceding
        ) as funding_rate_8h_avg_before,
        avg(funding_rate_8h) over (
            partition by symbol order by open_time_ms
            rows between 1 following and {{ window }} following
        ) as funding_rate_8h_avg_after,
        max(funding_rate_8h) over (
            partition by symbol order by open_time_ms
            rows between 1 following and {{ window }} following
        ) as funding_rate_8h_max_after
    from market

),

qualifying as (

    select
        symbol,
        open_time_ms,
        lag(open_time_ms) over (partition by symbol order by open_time_ms)
            as previous_qualifying_ms
    from market
    where return_1h >= {{ threshold }}

),

numbered as (

    select
        symbol,
        open_time_ms,
        sum(
            if(
                previous_qualifying_ms is null
                or open_time_ms - previous_qualifying_ms > {{ window }} * 3600000,
                1,
                0
            )
        ) over (
            partition by symbol order by open_time_ms
            rows between unbounded preceding and current row
        ) as episode_number
    from qualifying

),

episodes as (

    select
        symbol,
        episode_number,
        min(open_time_ms) as event_open_time_ms,
        max(open_time_ms) as episode_last_hour_ms,
        count(*) as episode_hours
    from numbered
    group by symbol, episode_number

),

events as (

    select
        *,
        lag(event_open_time_ms) over (partition by symbol order by event_open_time_ms)
            as previous_event_open_time_ms
    from episodes

),

flagged as (

    select
        events.*,
        around.*
            exclude (symbol),
        coalesce(
            around.window_start_ms
            = events.event_open_time_ms - {{ window + 1 }} * 3600000,
            false
        ) as has_full_before_window,
        coalesce(
            around.window_end_ms = events.event_open_time_ms + {{ window }} * 3600000,
            false
        ) as has_full_after_window
    from events
    inner join around
        on
            events.symbol = around.symbol
            and events.event_open_time_ms = around.open_time_ms

)

select
    {{ dbt_utils.generate_surrogate_key(['symbol', 'event_open_time_ms']) }}
        as pump_event_id,
    symbol,
    open_time as event_hour,
    event_open_time_ms,
    episode_hours,
    -- epoch_ms(ms) is the UTC wall clock; as timestamptz in the UTC session it is that moment.
    epoch_ms(episode_last_hour_ms)::timestamptz as episode_last_hour,
    -- The previous event's window reaches into this one's.
    coalesce(
        event_open_time_ms - previous_event_open_time_ms <= {{ 2 * window }} * 3600000,
        false
    ) as overlaps_previous_event,
    return_1h,
    return_4h,
    return_zscore,
    market_breadth,
    -- The scope rule lives here once: the profile and the dashboard read the column.
    if(market_breadth = 1, 'single_coin', 'multi_coin') as event_scope,
    log_volume_zscore,
    has_full_before_window,
    has_full_after_window,
    coalesce(
        has_full_before_window
        and has_full_after_window
        and open_interest_hours_in_window = {{ 2 * window + 2 }},
        false
    ) as has_full_open_interest_window,
    -- Anchors at the start of the event (end of hour -1) for the window mart.
    close_price_before,
    quote_volume_baseline,
    open_interest_coins_before,
    -- Price
    if(has_full_before_window, close_price_before / close_price_window_start - 1, null)
        as return_before,
    if(has_full_after_window, close_price_window_end / close_price - 1, null)
        as return_after,
    -- Volume, relative to the 7 days before the event
    quote_volume / quote_volume_baseline as volume_ratio_event,
    if(has_full_before_window, quote_volume_avg_before / quote_volume_baseline, null)
        as volume_ratio_before,
    if(has_full_after_window, quote_volume_avg_after / quote_volume_baseline, null)
        as volume_ratio_after,
    -- Open interest, in coins: positions, not their price
    if(
        has_full_before_window,
        open_interest_coins_before / nullif(open_interest_coins_window_start, 0) - 1,
        null
    ) as open_interest_change_before,
    open_interest_coins / nullif(open_interest_coins_before, 0) - 1
        as open_interest_change_event,
    if(
        has_full_after_window,
        open_interest_coins_window_end / nullif(open_interest_coins, 0) - 1,
        null
    ) as open_interest_change_after,
    -- Funding per 8 h; hourly as-of values, so an average is time-weighted
    funding_rate_8h_before,
    if(has_full_before_window, funding_rate_8h_avg_before, null)
        as funding_rate_8h_avg_before,
    if(has_full_after_window, funding_rate_8h_avg_after, null)
        as funding_rate_8h_avg_after,
    if(has_full_after_window, funding_rate_8h_max_after, null)
        as funding_rate_8h_max_after
from flagged
