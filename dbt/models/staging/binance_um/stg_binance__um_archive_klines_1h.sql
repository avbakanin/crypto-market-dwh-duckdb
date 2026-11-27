-- USD-M hourly candles of the universe's contracts from data.binance.vision (see the
-- macro): last price, with taker volume.
{{ config(
    materialized='incremental',
    unique_key=['symbol', 'open_time_ms'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

{{ stg_um_archive_klines('binance_um_archive_klines_1h') }}
