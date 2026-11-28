-- USD-M order book depth of the universe's contracts from data.binance.vision: each hour's
-- first snapshot (the extract keeps only it, binance_archive.first_snapshots), a row per
-- band. percentage is the distance from the mid in %, negative for bids, positive for asks;
-- depth (coins) and notional (USDT) are cumulative from the mid to the band.
-- timestamp is a naive UTC string; the snapshot comes a few seconds after the hour
-- (snapshot_lag_seconds). Incremental like the archive klines.
{{ config(
    materialized='incremental',
    unique_key=['contract_symbol', 'snapshot_hour_ms', 'percentage'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

with source as (

    select * from {{ source('binance', 'binance_um_archive_book_depth_1h') }}
    {% if is_incremental() %}
    where _batch_id > (select max(_batch_id) from {{ this }})
    {% endif %}

),

typed as (

    select
        symbol as contract_symbol,
        (payload->>'timestamp')::timestamp at time zone 'UTC' as snapshot_time,
        (payload->>'percentage')::decimal(6, 2) as percentage,
        (payload->>'depth')::decimal(38, 8) as depth,
        (payload->>'notional')::decimal(38, 8) as notional_usdt,
        _batch_id,
        _file_row_number,
        _loaded_at
    from source

)

select
    contract_symbol,
    date_trunc('hour', snapshot_time) as snapshot_hour,
    snapshot_time,
    datediff('second', date_trunc('hour', snapshot_time), snapshot_time)
        as snapshot_lag_seconds,
    percentage,
    case when percentage < 0 then 'bid' else 'ask' end as book_side,
    depth,
    notional_usdt,
    epoch_ms(date_trunc('hour', snapshot_time)) as snapshot_hour_ms,
    _batch_id,
    _loaded_at
from typed
qualify row_number() over (
    partition by contract_symbol, date_trunc('hour', snapshot_time), percentage
    order by _batch_id desc, _file_row_number desc
) = 1
