-- USD-M funding settlements from data.binance.vision (monthly files): typed, one row per
-- (contract, settlement hour). calc_time lands up to a few ms after the hour (note 132).
-- Incremental like the archive klines (macros/stg_um_archive_klines.sql).
{{ config(
    materialized='incremental',
    unique_key=['contract_symbol', 'funding_hour_ms'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

with source as (

    select * from {{ source('binance', 'binance_um_archive_funding_rate') }}
    {% if is_incremental() %}
    where _batch_id > (select max(_batch_id) from {{ this }})
    {% endif %}

)

select
    symbol as contract_symbol,
    epoch_ms((payload->>'calc_time')::bigint // 3600000 * 3600000)::timestamptz as funding_hour,
    (payload->>'last_funding_rate')::decimal(38, 8) as funding_rate,
    (payload->>'funding_interval_hours')::integer as interval_hours,
    (payload->>'calc_time')::bigint // 3600000 * 3600000 as funding_hour_ms,
    (payload->>'calc_time')::bigint as calc_time_ms,
    _batch_id,
    _loaded_at
from source
qualify row_number() over (
    partition by symbol, (payload->>'calc_time')::bigint // 3600000
    order by _batch_id desc, _file_row_number desc
) = 1
