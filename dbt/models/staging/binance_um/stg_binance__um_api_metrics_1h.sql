-- USD-M hourly open interest and long/short ratios of the universe from the API: four
-- series, one row per (contract, hour), the latest batch of each. The API's ratios come
-- from shares rounded to 4 decimals (note 130): they differ from the archive's by ~1e-3.

{% set ratio_series = {
    'top_account_ratio': 'binance_um_api_top_account_ratio_1h',
    'top_position_ratio': 'binance_um_api_top_position_ratio_1h',
    'global_account_ratio': 'binance_um_api_global_account_ratio_1h',
} %}

with open_interest as (

    select
        symbol as contract_symbol,
        (payload->>'timestamp')::bigint as snapshot_time_ms,
        (payload->>'sumOpenInterest')::decimal(38, 8) as open_interest,
        (payload->>'sumOpenInterestValue')::decimal(38, 8) as open_interest_usdt
    from {{ source('binance', 'binance_um_api_open_interest_1h') }}
    qualify row_number() over (
        partition by symbol, (payload->>'timestamp')::bigint
        order by _batch_id desc, _file_row_number desc
    ) = 1

),

{% for name, table in ratio_series.items() %}
{{ name }} as (

    select
        symbol as contract_symbol,
        (payload->>'timestamp')::bigint as snapshot_time_ms,
        (payload->>'longShortRatio')::decimal(38, 8) as {{ name }}
    from {{ source('binance', table) }}
    qualify row_number() over (
        partition by symbol, (payload->>'timestamp')::bigint
        order by _batch_id desc, _file_row_number desc
    ) = 1

),
{% endfor %}

keys as (

    select contract_symbol, snapshot_time_ms from open_interest
    {% for name in ratio_series %}
    union
    select contract_symbol, snapshot_time_ms from {{ name }}
    {% endfor %}

)

select
    keys.contract_symbol,
    epoch_ms(keys.snapshot_time_ms)::timestamptz as snapshot_time,
    open_interest.open_interest,
    open_interest.open_interest_usdt,
    {% for name in ratio_series %}
    {{ name }}.{{ name }},
    {% endfor %}
    keys.snapshot_time_ms
from keys
left join open_interest using (contract_symbol, snapshot_time_ms)
{% for name in ratio_series %}
left join {{ name }} using (contract_symbol, snapshot_time_ms)
{% endfor %}
