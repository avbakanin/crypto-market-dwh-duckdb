-- Coverage of the liquidation stream, one row per hour that has connection records: the
-- minutes with any record (a heartbeat every 20 s while connected). An hour with no row was
-- not collected at all; liquidations of an hour are complete only at 60 minutes.

with source as (

    select (payload->>'r')::bigint as received_ms, payload->>'c' as record
    from {{ source('binance', 'binance_um_liquidations') }}
    where symbol = 'STREAM'

)

select
    epoch_ms(received_ms // 3600000 * 3600000)::timestamptz as stream_hour,
    received_ms // 3600000 * 3600000 as stream_hour_ms,
    count(distinct received_ms // 60000) as connected_minutes,
    count(*) filter (where record = 'open') as opens,
    count(*) filter (where record = 'close') as closes
from source
group by all
