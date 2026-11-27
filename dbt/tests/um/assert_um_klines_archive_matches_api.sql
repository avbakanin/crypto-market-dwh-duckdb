{{ config(severity='warn', store_failures_as='table', alias='dq_um_klines_archive_vs_api') }}

-- Warn: an hour both the archive and the API have, with any value different. Both are
-- the same exchange's closed candles, so they must be equal; the stitched model takes the
-- archive, and the live signal reads the API tail: a difference is a train/serve skew.

select
    a.symbol,
    a.open_time,
    a.open_price, p.open_price as api_open_price,
    a.high_price, p.high_price as api_high_price,
    a.low_price, p.low_price as api_low_price,
    a.close_price, p.close_price as api_close_price,
    a.quote_volume, p.quote_volume as api_quote_volume,
    a.trade_count, p.trade_count as api_trade_count,
    a.taker_buy_quote_volume, p.taker_buy_quote_volume as api_taker_buy_quote_volume
from {{ ref('stg_binance__um_archive_klines_1h') }} as a
inner join {{ ref('stg_binance__um_api_klines_1h') }} as p using (symbol, open_time_ms)
where a.open_price <> p.open_price
    or a.high_price <> p.high_price
    or a.low_price <> p.low_price
    or a.close_price <> p.close_price
    or a.base_volume <> p.base_volume
    or a.quote_volume <> p.quote_volume
    or a.trade_count <> p.trade_count
    or a.taker_buy_base_volume <> p.taker_buy_base_volume
    or a.taker_buy_quote_volume <> p.taker_buy_quote_volume
