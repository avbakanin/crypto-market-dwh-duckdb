-- USD-M hourly candles of the universe: the archive where it has the hour, the API for
-- the hours after (the archive publishes a day the morning after). One row per (symbol,
-- open_time); `source` says which. Where both have an hour they must agree
-- (tests/um/assert_um_klines_archive_matches_api.sql).

with archive as (

    select
        symbol, open_time, close_time, open_price, high_price, low_price, close_price,
        base_volume, quote_volume, trade_count, taker_buy_base_volume,
        taker_buy_quote_volume, open_time_ms
    from {{ ref('stg_binance__um_archive_klines_1h') }}

),

api as (

    select
        symbol, open_time, close_time, open_price, high_price, low_price, close_price,
        base_volume, quote_volume, trade_count, taker_buy_base_volume,
        taker_buy_quote_volume, open_time_ms
    from {{ ref('stg_binance__um_api_klines_1h') }} as a
    where not exists (
        select 1 from archive
        where archive.symbol = a.symbol and archive.open_time_ms = a.open_time_ms
    )

)

select *, 'archive' as source from archive
union all
select *, 'api' as source from api
