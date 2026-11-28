-- USD-M hourly premium index candles of the universe's contracts from data.binance.vision:
-- the premium of the contract over its index, sampled every 5 s (sample_count, 720 an
-- hour; 0 on a delisted contract's idle tail). The basis behind funding, not clamped.
-- Incremental like the archive klines (macros/stg_um_archive_klines.sql).
{{ config(
    materialized='incremental',
    unique_key=['symbol', 'open_time_ms'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

with source as (

    select * from {{ source('binance', 'binance_um_archive_premium_index_1h') }}
    {% if is_incremental() %}
    where _batch_id > (select max(_batch_id) from {{ this }})
    {% endif %}

)

select
    symbol,
    epoch_ms((payload->>'open_time')::bigint)::timestamptz as open_time,
    (payload->>'open')::decimal(38, 8) as open_premium,
    (payload->>'high')::decimal(38, 8) as high_premium,
    (payload->>'low')::decimal(38, 8) as low_premium,
    (payload->>'close')::decimal(38, 8) as close_premium,
    (payload->>'count')::bigint as sample_count,
    (payload->>'open_time')::bigint as open_time_ms,
    (payload->>'close_time')::bigint as close_time_ms,
    _batch_id,
    _loaded_at
from source
qualify row_number() over (
    partition by symbol, (payload->>'open_time')::bigint
    order by _batch_id desc, _file_row_number desc
) = 1
