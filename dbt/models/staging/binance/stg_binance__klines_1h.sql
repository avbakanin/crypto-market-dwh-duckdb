-- Spot 1h candles: typed, UTC, one row per (symbol, open_time).
-- RAW keeps every version of a candle (overlapping batches); the latest batch wins,
-- not the latest load: an older batch loaded late must not override a newer one.

with source as (

    select * from {{ source('binance', 'binance_spot_klines_1h') }}

),

typed as (

    select
        symbol,
        -- Unix ms as bigint; epoch_ms() gives a timestamp, read as UTC by the cast to
        -- timestamptz in the profile's UTC session (the epoch round trip test checks it).
        (payload->>0)::bigint as open_time_ms,
        (payload->>6)::bigint as close_time_ms,
        epoch_ms((payload->>0)::bigint)::timestamptz as open_time,
        epoch_ms((payload->>6)::bigint)::timestamptz as close_time,
        -- Prices and volumes arrive as JSON strings: exact decimal cast, never float.
        (payload->>1)::decimal(38, 8) as open_price,
        (payload->>2)::decimal(38, 8) as high_price,
        (payload->>3)::decimal(38, 8) as low_price,
        (payload->>4)::decimal(38, 8) as close_price,
        (payload->>5)::decimal(38, 8) as base_volume,
        (payload->>7)::decimal(38, 8) as quote_volume,
        (payload->>8)::bigint as trade_count,
        (payload->>9)::decimal(38, 8) as taker_buy_base_volume,
        (payload->>10)::decimal(38, 8) as taker_buy_quote_volume,
        payload,
        _batch_id,
        _file_row_number,
        _loaded_at
    from source

),

versioned as (

    select
        *,
        count(*) over (partition by symbol, open_time_ms) as _version_count,
        -- Closed candles never change (source note 27): differing payloads for one key
        -- mean the source rewrote history, or a candle was taken before it closed.
        min(hash(payload)) over (partition by symbol, open_time_ms)
        <> max(hash(payload)) over (partition by symbol, open_time_ms) as _is_revised
    from typed
    qualify row_number() over (
        partition by symbol, open_time_ms
        order by _batch_id desc, _file_row_number desc
    ) = 1

)

select
    symbol,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    base_volume,
    quote_volume,
    trade_count,
    taker_buy_base_volume,
    taker_buy_quote_volume,
    open_time_ms,
    close_time_ms,
    _version_count,
    _is_revised,
    _batch_id,
    _loaded_at
from versioned
