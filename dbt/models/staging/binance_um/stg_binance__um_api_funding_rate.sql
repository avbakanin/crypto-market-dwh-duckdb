-- USD-M funding settlements of the universe from the API (this month: the archive keeps
-- funding monthly only). One row per (contract, settlement hour), the latest batch.

with source as (

    select * from {{ source('binance', 'binance_um_api_funding_rate') }}

)

select
    symbol as contract_symbol,
    epoch_ms((payload->>'fundingTime')::bigint // 3600000 * 3600000)::timestamptz as funding_hour,
    (payload->>'fundingRate')::decimal(38, 8) as funding_rate,
    (payload->>'fundingTime')::bigint // 3600000 * 3600000 as funding_hour_ms,
    _batch_id,
    _loaded_at
from source
qualify row_number() over (
    partition by symbol, (payload->>'fundingTime')::bigint // 3600000
    order by _batch_id desc, _file_row_number desc
) = 1
