-- USD-M 1h candles of the universe from the API (the tail the archive has not published):
-- the kline array of the spot layout, one row per (symbol, open_time), the latest batch.

with source as (

    select * from {{ source('binance', 'binance_um_api_klines_1h') }}

)

select
    symbol,
    epoch_ms((payload->>0)::bigint)::timestamptz as open_time,
    epoch_ms((payload->>6)::bigint)::timestamptz as close_time,
    (payload->>1)::decimal(38, 8) as open_price,
    (payload->>2)::decimal(38, 8) as high_price,
    (payload->>3)::decimal(38, 8) as low_price,
    (payload->>4)::decimal(38, 8) as close_price,
    (payload->>5)::decimal(38, 8) as base_volume,
    (payload->>7)::decimal(38, 8) as quote_volume,
    (payload->>8)::bigint as trade_count,
    (payload->>9)::decimal(38, 8) as taker_buy_base_volume,
    (payload->>10)::decimal(38, 8) as taker_buy_quote_volume,
    (payload->>0)::bigint as open_time_ms,
    (payload->>6)::bigint as close_time_ms,
    _batch_id,
    _loaded_at
from source
qualify row_number() over (
    partition by symbol, (payload->>0)::bigint
    order by _batch_id desc, _file_row_number desc
) = 1
