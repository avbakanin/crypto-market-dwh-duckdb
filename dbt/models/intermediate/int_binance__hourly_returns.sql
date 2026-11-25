-- One row per (symbol, hour): returns and trailing baselines for the pump rule.
-- Returns check the time step, so a hole in the grid never yields a return across it.
-- Baselines count rows, not hours: the staging grid test (warn) watches completeness,
-- and a rare hole only stretches a 30-day baseline by an hour.
-- Baselines exclude the current hour: a pump must not dilute its own z-score.

{%- set zscore_window = var('return_zscore_window_hours') %}
{%- set zscore_min = var('return_zscore_min_hours') %}
{%- set volume_window = var('volume_baseline_hours') %}

with klines as (

    select
        symbol,
        open_time,
        open_time_ms,
        close_price,
        quote_volume,
        taker_buy_quote_volume,
        trade_count
    from {{ ref('stg_binance__klines_1h') }}

),

lagged as (

    select
        *,
        lag(open_time_ms) over (partition by symbol order by open_time_ms)
            as previous_open_time_ms,
        lag(close_price) over (partition by symbol order by open_time_ms)
            as previous_close_price,
        lag(open_time_ms, 4) over (partition by symbol order by open_time_ms)
            as open_time_ms_4h_ago,
        lag(close_price, 4) over (partition by symbol order by open_time_ms)
            as close_price_4h_ago
    from klines

),

returns as (

    select
        symbol,
        open_time,
        open_time_ms,
        close_price,
        quote_volume,
        taker_buy_quote_volume,
        trade_count,
        -- Returns are compared with thresholds (5 % pump, 2 % flat price): kept as
        -- decimal(38, 12), the scale Snowflake gave this division. In DuckDB the division is
        -- a DOUBLE, and a return of exactly -2 % came out as -0.0199999999999996 (source
        -- note 111); rounding to 12 places restores the exact decimal.
        if(
            open_time_ms - previous_open_time_ms = 3600000,
            (close_price / previous_close_price - 1)::decimal(38, 12),
            null
        ) as return_1h,
        if(
            open_time_ms - open_time_ms_4h_ago = 4 * 3600000,
            (close_price / close_price_4h_ago - 1)::decimal(38, 12),
            null
        ) as return_4h,
        -- Volume has a heavy right tail: a z-score of raw volume flags ~1800 hours a year,
        -- of its logarithm ~24. Volume is never 0 in the data; nullif keeps ln defined.
        ln(nullif(quote_volume, 0)) as log_quote_volume
    from lagged

),

baselines as (

    select
        *,
        avg(return_1h) over (
            partition by symbol order by open_time_ms
            rows between {{ zscore_window }} preceding and 1 preceding
        ) as return_baseline_mean,
        stddev_samp(return_1h) over (
            partition by symbol order by open_time_ms
            rows between {{ zscore_window }} preceding and 1 preceding
        ) as return_baseline_stddev,
        count(return_1h) over (
            partition by symbol order by open_time_ms
            rows between {{ zscore_window }} preceding and 1 preceding
        ) as return_baseline_hours,
        avg(quote_volume) over (
            partition by symbol order by open_time_ms
            rows between {{ volume_window }} preceding and 1 preceding
        ) as volume_baseline_mean,
        avg(log_quote_volume) over (
            partition by symbol order by open_time_ms
            rows between {{ volume_window }} preceding and 1 preceding
        ) as log_volume_baseline_mean,
        stddev_samp(log_quote_volume) over (
            partition by symbol order by open_time_ms
            rows between {{ volume_window }} preceding and 1 preceding
        ) as log_volume_baseline_stddev,
        count(log_quote_volume) over (
            partition by symbol order by open_time_ms
            rows between {{ volume_window }} preceding and 1 preceding
        ) as volume_baseline_hours
    from returns

)

select
    symbol,
    open_time,
    open_time_ms,
    close_price,
    quote_volume,
    taker_buy_quote_volume,
    -- Aggressive buyers' share of the hour's volume: about 0.5 normally (median 0.494).
    taker_buy_quote_volume / nullif(quote_volume, 0) as taker_buy_share,
    trade_count,
    return_1h,
    return_4h,
    -- z-scores are statistics for ranking and flags, not stored facts: float is fine.
    if(
        return_baseline_hours >= {{ zscore_min }} and return_baseline_stddev > 0,
        (return_1h - return_baseline_mean) / return_baseline_stddev,
        null
    ) as return_zscore,
    return_baseline_hours,
    -- Volume baselines only over a full window: a 3-hour "week" is not a baseline.
    if(
        volume_baseline_hours = {{ volume_window }},
        volume_baseline_mean,
        null
    ) as quote_volume_baseline,
    if(
        volume_baseline_hours = {{ volume_window }} and log_volume_baseline_stddev > 0,
        (log_quote_volume - log_volume_baseline_mean) / log_volume_baseline_stddev,
        null
    ) as log_volume_zscore
from baselines
