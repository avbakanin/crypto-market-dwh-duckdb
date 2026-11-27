-- USD-M USDT contracts as exchangeInfo lists them in the newest snapshot that has each
-- (ingestion/extract_um_api.py writes one when the list changes). A contract delisted long
-- ago is in no snapshot: the universe treats an unknown contract as a coin.

with source as (

    select * from {{ source('binance', 'binance_um_contracts') }}

)

select
    payload->>'symbol' as contract_symbol,
    payload->>'contractType' as contract_type,
    payload->>'underlyingType' as underlying_type,
    payload->>'status' as status,
    epoch_ms((payload->>'onboardDate')::bigint)::timestamptz as onboard_time,
    payload->>'underlyingSubType' as underlying_sub_type,
    _batch_id,
    _loaded_at
from source
qualify row_number() over (
    partition by payload->>'symbol'
    order by _batch_id desc, _file_row_number desc
) = 1
