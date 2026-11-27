-- USD-M daily candles of every *USDT perpetual from data.binance.vision (see the macro):
-- last price, with taker volume. The universe ranks contracts by them.
{{ config(
    materialized='incremental',
    unique_key=['symbol', 'open_time_ms'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

{{ stg_um_archive_klines('binance_um_archive_klines_1d') }}
