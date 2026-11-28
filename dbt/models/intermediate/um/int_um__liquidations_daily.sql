-- USD-M liquidations per (contract, UTC day) from the stream, with the stream's coverage of
-- the day (connected minutes of 1440). A contract without liquidations on a covered day has
-- no row: its liquidations are 0 only when coverage_minutes is 1440 (a lower bound anyway,
-- one message per contract per second). Days before the collector started have no rows.

with liquidations as (

    select
        contract_symbol,
        trade_time::date as liquidation_date,
        sum(notional_usdt) filter (where liquidated_side = 'long') as long_liquidated_usdt,
        sum(notional_usdt) filter (where liquidated_side = 'short') as short_liquidated_usdt,
        count(*) filter (where liquidated_side = 'long') as long_liquidations,
        count(*) filter (where liquidated_side = 'short') as short_liquidations
    from {{ ref('stg_binance__um_liquidations') }}
    group by all

),

coverage as (

    select stream_hour::date as liquidation_date, sum(connected_minutes) as coverage_minutes
    from {{ ref('stg_binance__um_liquidation_stream') }}
    group by all

)

select
    l.contract_symbol,
    l.liquidation_date,
    coalesce(l.long_liquidated_usdt, 0)::decimal(38, 12) as long_liquidated_usdt,
    coalesce(l.short_liquidated_usdt, 0)::decimal(38, 12) as short_liquidated_usdt,
    l.long_liquidations,
    l.short_liquidations,
    coalesce(c.coverage_minutes, 0) as coverage_minutes
from liquidations as l
left join coverage as c using (liquidation_date)
