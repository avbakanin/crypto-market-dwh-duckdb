-- USD-M hourly open interest: typed, UTC, one row per (contract, snapshot hour).
-- RAW is the only copy past ~30 days (source note 36): never filtered here.

with source as (

    select * from {{ source('binance', 'binance_futures_open_interest_1h') }}

),

typed as (

    select
        symbol as contract_symbol,
        (payload->>'timestamp')::bigint as snapshot_time_ms,
        (payload->>'sumOpenInterest')::decimal(38, 8) as open_interest,
        (payload->>'sumOpenInterestValue')::decimal(38, 8) as open_interest_usdt,
        (payload->>'CMCCirculatingSupply')::decimal(38, 8) as cmc_circulating_supply,
        payload,
        _batch_id,
        _file_row_number,
        _loaded_at
    from source

),

versioned as (

    select
        *,
        count(*) over (partition by contract_symbol, snapshot_time_ms) as _version_count,
        min(hash(payload)) over (partition by contract_symbol, snapshot_time_ms)
        <> max(hash(payload)) over (partition by contract_symbol, snapshot_time_ms)
            as _is_revised
    from typed
    qualify row_number() over (
        partition by contract_symbol, snapshot_time_ms
        order by _batch_id desc, _file_row_number desc
    ) = 1

)

select
    versioned.contract_symbol,
    perpetuals.spot_symbol,
    epoch_ms(versioned.snapshot_time_ms)::timestamptz as snapshot_time,
    -- In contract units (1000PEPE for 1000PEPEUSDT); coins = units * multiplier.
    versioned.open_interest,
    versioned.open_interest * perpetuals.contract_multiplier as open_interest_coins,
    versioned.open_interest_usdt,
    versioned.cmc_circulating_supply,
    perpetuals.contract_multiplier,
    versioned.snapshot_time_ms,
    versioned._version_count,
    versioned._is_revised,
    versioned._batch_id,
    versioned._loaded_at
from versioned
left join {{ ref('perpetuals') }} as perpetuals
    on versioned.contract_symbol = perpetuals.futures_symbol
