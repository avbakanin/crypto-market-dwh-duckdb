-- USD-M hourly open interest and long/short ratios from data.binance.vision: the rows at
-- minute 00 of the 5-minute files (the extract keeps only those), one row per (contract,
-- hour). The file's column names are wrong; mapped by source note 130 against the API:
--   count_toptrader_long_short_ratio  top traders, by accounts
--   sum_toptrader_long_short_ratio    top traders, by position
--   count_long_short_ratio            all traders, by accounts
--   sum_taker_long_short_vol_ratio    taker buy / sell volume
-- create_time is a naive UTC string; files repeat and reorder rows (note 131).
-- Files before 2026-11-16 stamp each 5-minute snapshot 5 minutes early (note 137): their
-- row at minute 00 holds the snapshot the API stamps minute 05. snapshot_lag_minutes says
-- so (5, else 0); the minute-55 row it would take is not extracted.
-- Incremental like the archive klines (macros/stg_um_archive_klines.sql).
{{ config(
    materialized='incremental',
    unique_key=['contract_symbol', 'snapshot_time_ms'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

with source as (

    select * from {{ source('binance', 'binance_um_archive_metrics_1h') }}
    {% if is_incremental() %}
    where _batch_id > (select max(_batch_id) from {{ this }})
    {% endif %}

),

typed as (

    select
        symbol as contract_symbol,
        (payload->>'create_time')::timestamp at time zone 'UTC' as snapshot_time,
        nullif(payload->>'sum_open_interest', '')::decimal(38, 8) as open_interest,
        nullif(payload->>'sum_open_interest_value', '')::decimal(38, 8) as open_interest_usdt,
        nullif(payload->>'count_toptrader_long_short_ratio', '')::decimal(38, 12)
            as top_account_ratio,
        nullif(payload->>'sum_toptrader_long_short_ratio', '')::decimal(38, 12)
            as top_position_ratio,
        nullif(payload->>'count_long_short_ratio', '')::decimal(38, 12) as global_account_ratio,
        nullif(payload->>'sum_taker_long_short_vol_ratio', '')::decimal(38, 12)
            as taker_buy_sell_ratio,
        _batch_id,
        _file_row_number,
        _loaded_at
    from source

)

select
    contract_symbol,
    snapshot_time,
    open_interest,
    open_interest_usdt,
    top_account_ratio,
    top_position_ratio,
    global_account_ratio,
    taker_buy_sell_ratio,
    case when snapshot_time < timestamptz '2026-11-16 00:00:00+00' then 5 else 0 end
        as snapshot_lag_minutes,
    epoch_ms(snapshot_time) as snapshot_time_ms,
    _batch_id,
    _loaded_at
from typed
qualify row_number() over (
    partition by contract_symbol, snapshot_time
    order by _batch_id desc, _file_row_number desc
) = 1
