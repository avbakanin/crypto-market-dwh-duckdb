-- USD-M funding settlements: typed, UTC, one row per (contract, settlement hour).
-- fundingTime lands 0-26 ms after the hour (source note 33): the hour is the key, the
-- same key the extract and the loader count duplicates on.
-- Missed settlements are flagged, never filled in: a gap is a fact about the source.

with source as (

    select * from {{ source('binance', 'binance_futures_funding_rate') }}

),

typed as (

    select
        symbol as contract_symbol,
        (payload->>'fundingTime')::bigint as funding_time_ms,
        -- Integer division: / would give a DOUBLE in DuckDB.
        (payload->>'fundingTime')::bigint // 3600000 * 3600000 as funding_hour_ms,
        (payload->>'fundingRate')::decimal(38, 8) as funding_rate,
        -- Old records carry markPrice = "" (source note 32).
        nullif(payload->>'markPrice', '')::decimal(38, 8) as mark_price,
        payload,
        _batch_id,
        _file_row_number,
        _loaded_at
    from source

),

versioned as (

    select
        *,
        count(*) over (partition by contract_symbol, funding_hour_ms) as _version_count,
        min(hash(payload)) over (partition by contract_symbol, funding_hour_ms)
        <> max(hash(payload)) over (partition by contract_symbol, funding_hour_ms)
            as _is_revised
    from typed
    qualify row_number() over (
        partition by contract_symbol, funding_hour_ms
        order by _batch_id desc, _file_row_number desc
    ) = 1

),

timed as (

    select
        *,
        epoch_ms(funding_hour_ms)::timestamptz as funding_hour,
        epoch_ms(funding_time_ms)::timestamptz as funding_time
    from versioned

),

stepped as (

    select
        *,
        datediff(
            'hour',
            lag(funding_hour) over (partition by contract_symbol order by funding_hour),
            funding_hour
        ) as hours_since_previous,
        datediff(
            'hour',
            funding_hour,
            lead(funding_hour) over (partition by contract_symbol order by funding_hour)
        ) as hours_until_next
    from timed

),

intervals as (

    select
        *,
        -- The interval is not in the data and its history is not served (source note 34):
        -- the shorter step around a settlement. A single missed settlement makes one step
        -- twice as long, the other step still shows the interval.
        least(
            coalesce(hours_since_previous, hours_until_next),
            coalesce(hours_until_next, hours_since_previous)
        ) as interval_hours
    from stepped

)

select
    intervals.contract_symbol,
    perpetuals.spot_symbol,
    intervals.funding_hour,
    intervals.funding_time,
    intervals.funding_rate,
    intervals.mark_price,
    -- 1000PEPEUSDT is quoted per 1000 coins: per-coin price comparable with spot.
    -- A DOUBLE in DuckDB (decimal / decimal), exact in Snowflake: compared with a tolerance.
    intervals.mark_price / perpetuals.contract_multiplier as mark_price_per_coin,
    intervals.interval_hours,
    intervals.hours_since_previous,
    coalesce(intervals.hours_since_previous > intervals.interval_hours, false)
        as is_after_missed_settlement,
    perpetuals.contract_multiplier,
    perpetuals.interest_rate_8h,
    intervals.funding_hour_ms,
    intervals.funding_time_ms,
    intervals._version_count,
    intervals._is_revised,
    intervals._batch_id,
    intervals._loaded_at
from intervals
left join {{ ref('perpetuals') }} as perpetuals
    on intervals.contract_symbol = perpetuals.futures_symbol
