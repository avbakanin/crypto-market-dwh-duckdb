{{ config(severity='warn', store_failures_as='table', alias='dq_um_daily_bars_vs_1d') }}

-- Warn (source to target): a daily bar built from 24 hourly candles differs from the
-- archive's own daily candle in open/high/low/close, trades, or quote / taker buy volume
-- beyond 1e-6 relative. The archive's 1h and 1d files disagree on ~300 of 658k
-- contract-days (2020-01, 2021-02/03, 2022-06, 2023-08-16 and a few more; close equal):
-- source note 139. Growth means the aggregation or a new file is off.

with bars as (

    select * from {{ ref('int_um__daily_bars') }} where hours_present = 24

),

daily as (

    select
        symbol,
        open_time::date as trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        quote_volume,
        taker_buy_quote_volume,
        trade_count
    from {{ ref('stg_binance__um_archive_klines_1d') }}

)

select
    bars.symbol,
    bars.trade_date,
    bars.close_price as bar_close,
    daily.close_price as daily_close,
    bars.quote_volume as bar_quote_volume,
    daily.quote_volume as daily_quote_volume,
    bars.trade_count as bar_trades,
    daily.trade_count as daily_trades
from bars
inner join daily using (symbol, trade_date)
where bars.open_price <> daily.open_price
    or bars.high_price <> daily.high_price
    or bars.low_price <> daily.low_price
    or bars.close_price <> daily.close_price
    or bars.trade_count <> daily.trade_count
    or abs(bars.quote_volume - daily.quote_volume) > 1e-6 * daily.quote_volume + 1e-6
    or abs(bars.taker_buy_quote_volume - daily.taker_buy_quote_volume)
        > 1e-6 * daily.taker_buy_quote_volume + 1e-6
