{{ config(severity='warn', store_failures_as='table', alias='dq_spot_futures_divergence') }}

-- Spot and perpetual prices of one coin at each funding settlement: the spot close of the
-- candle ending at the settlement against the contract's mark price per coin (1000PEPEUSDT
-- is quoted per 1000 coins). More than 2 % apart is either a data error (a wrong contract
-- or multiplier) or a market event worth a look. Binance offers no second exchange here:
-- spot against futures is the cross-check (BingX was left out of the project).

{% set max_divergence = 0.02 %}

with settlements as (

    select spot_symbol, contract_symbol, funding_hour, mark_price_per_coin
    from {{ ref('stg_binance__funding_rate') }}
    where mark_price_per_coin is not null

),

candles as (

    select symbol, close_time, open_time, close_price
    from {{ ref('stg_binance__klines_1h') }}

)

select
    settlements.spot_symbol,
    settlements.contract_symbol,
    settlements.funding_hour,
    candles.close_price as spot_close,
    settlements.mark_price_per_coin,
    settlements.mark_price_per_coin / candles.close_price - 1 as divergence
from settlements
inner join candles
    on settlements.spot_symbol = candles.symbol
    -- The candle that closes at the settlement: [hour - 1 h, hour - 1 ms].
    and candles.open_time = settlements.funding_hour - interval 1 hour
where abs(settlements.mark_price_per_coin / candles.close_price - 1) > {{ max_divergence }}
