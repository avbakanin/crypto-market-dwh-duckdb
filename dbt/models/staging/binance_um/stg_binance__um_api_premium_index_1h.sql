-- USD-M hourly premium index candles of the universe from the API (the tail the archive
-- has not published): the kline array layout, one row per (symbol, open_time), the latest
-- batch. Same columns as stg_binance__um_archive_premium_index_1h.

with source as (

    select * from {{ source('binance', 'binance_um_api_premium_index_1h') }}

)

select
    symbol,
    epoch_ms((payload->>0)::bigint)::timestamptz as open_time,
    (payload->>1)::decimal(38, 8) as open_premium,
    (payload->>2)::decimal(38, 8) as high_premium,
    (payload->>3)::decimal(38, 8) as low_premium,
    (payload->>4)::decimal(38, 8) as close_premium,
    (payload->>8)::bigint as sample_count,
    (payload->>0)::bigint as open_time_ms,
    (payload->>6)::bigint as close_time_ms,
    _batch_id,
    _loaded_at
from source
qualify row_number() over (
    partition by symbol, (payload->>0)::bigint
    order by _batch_id desc, _file_row_number desc
) = 1
