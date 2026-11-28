-- USD-M liquidations from the stream, one row per distinct message. order_side SELL closes
-- a long (longs liquidated), BUY closes a short. notional = filled quantity x average price.

with source as (

    select * from {{ source('binance', 'binance_um_liquidations') }}
    where symbol <> 'STREAM'

)

select
    symbol as contract_symbol,
    epoch_ms((payload->'o'->>'T')::bigint)::timestamptz as trade_time,
    (payload->'o'->>'T')::bigint as trade_time_ms,
    (payload->>'E')::bigint as event_time_ms,
    payload->'o'->>'S' as order_side,
    case payload->'o'->>'S' when 'SELL' then 'long' when 'BUY' then 'short' end as liquidated_side,
    payload->'o'->>'X' as order_status,
    (payload->'o'->>'z')::decimal(38, 12) as filled_qty,
    (payload->'o'->>'ap')::decimal(38, 12) as avg_price,
    ((payload->'o'->>'z')::decimal(38, 12) * (payload->'o'->>'ap')::decimal(38, 12))::decimal(38, 12)
        as notional_usdt,
    _batch_id,
    _loaded_at
from source
qualify row_number() over (
    partition by symbol, payload::varchar
    order by _batch_id, _file_row_number
) = 1
