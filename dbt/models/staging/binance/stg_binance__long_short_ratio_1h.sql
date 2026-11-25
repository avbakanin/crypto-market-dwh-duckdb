-- USD-M hourly long/short ratios of three trader populations: typed, UTC, one row per
-- (contract, series, snapshot hour). One RAW table per series, the same payload shape, so
-- the series comes from the table. RAW is the only copy past ~30 days: never filtered.
-- Shares and ratios do not depend on the contract size: no multiplier here.

{%- set series = [
    ('top_account', 'binance_futures_top_account_ratio_1h'),
    ('top_position', 'binance_futures_top_position_ratio_1h'),
    ('global_account', 'binance_futures_global_account_ratio_1h'),
] %}

with source as (

    {% for name, table in series %}
    select '{{ name }}' as ratio_series, * from {{ source('binance', table) }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

),

typed as (

    select
        symbol as contract_symbol,
        ratio_series,
        (payload->>'timestamp')::bigint as snapshot_time_ms,
        (payload->>'longAccount')::decimal(38, 8) as long_share,
        (payload->>'shortAccount')::decimal(38, 8) as short_share,
        (payload->>'longShortRatio')::decimal(38, 8) as long_short_ratio,
        payload,
        _batch_id,
        _file_row_number,
        _loaded_at
    from source

),

versioned as (

    select
        *,
        count(*) over (partition by contract_symbol, ratio_series, snapshot_time_ms)
            as _version_count,
        min(hash(payload)) over (partition by contract_symbol, ratio_series, snapshot_time_ms)
        <> max(hash(payload)) over (partition by contract_symbol, ratio_series, snapshot_time_ms)
            as _is_revised
    from typed
    qualify row_number() over (
        partition by contract_symbol, ratio_series, snapshot_time_ms
        order by _batch_id desc, _file_row_number desc
    ) = 1

)

select
    versioned.contract_symbol,
    perpetuals.spot_symbol,
    versioned.ratio_series,
    epoch_ms(versioned.snapshot_time_ms)::timestamptz as snapshot_time,
    -- Shares of the population (accounts or position size) long and short, rounded to 4
    -- decimals by Binance; the ratio is long / short of the unrounded shares.
    versioned.long_share,
    versioned.short_share,
    versioned.long_short_ratio,
    versioned.snapshot_time_ms,
    versioned._version_count,
    versioned._is_revised,
    versioned._batch_id,
    versioned._loaded_at
from versioned
left join {{ ref('perpetuals') }} as perpetuals
    on versioned.contract_symbol = perpetuals.futures_symbol
