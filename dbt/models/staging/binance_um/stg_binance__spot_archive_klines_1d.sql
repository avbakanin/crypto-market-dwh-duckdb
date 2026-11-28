-- Daily spot candles of the pairs that may trade the universe's coins, from
-- data.binance.vision (see the macro; times in microseconds from 2025 are taken to ms).
-- `symbol` is the spot pair; int_um__spot_daily maps it to its contract.
{{ config(
    materialized='incremental',
    unique_key=['symbol', 'open_time_ms'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

{{ stg_um_archive_klines('binance_spot_archive_klines_1d') }}
