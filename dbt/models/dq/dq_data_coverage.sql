-- How far each extract dataset reaches, one row per dataset (its landing-zone name):
-- the dashboard's "data as of" and its coverage table. data_from and data_to are moments:
-- the first and last point of the series, for candles the end of the last closed hour
-- (open + 1 h), the moment the marts' end-of-hour rows describe. latest_batch_id: the
-- newest extract batch among the kept versions.
-- Every dataset gets a row even when its staging model is empty (row_count 0): a dataset
-- that disappears fails a test instead of dropping out of the table.

{%- set ratio_series = ['top_account', 'top_position', 'global_account'] %}

with datasets as (

    select dataset
    from (values
        ('klines_1h'),
        ('funding_rate'),
        ('open_interest_1h'),
        {%- for series in ratio_series %}
        ('{{ series }}_ratio_1h'){{ ',' if not loop.last }}
        {%- endfor %}
    ) as v(dataset)

),

klines as (

    select
        'klines_1h' as dataset,
        count(*) as row_count,
        count(distinct symbol) as symbol_count,
        min(open_time) as data_from,
        max(open_time) + interval 1 hour as data_to,
        max(_batch_id) as latest_batch_id
    from {{ ref('stg_binance__klines_1h') }}

),

funding as (

    select
        'funding_rate' as dataset,
        count(*) as row_count,
        count(distinct contract_symbol) as symbol_count,
        min(funding_hour) as data_from,
        max(funding_hour) as data_to,
        max(_batch_id) as latest_batch_id
    from {{ ref('stg_binance__funding_rate') }}

),

open_interest as (

    select
        'open_interest_1h' as dataset,
        count(*) as row_count,
        count(distinct contract_symbol) as symbol_count,
        min(snapshot_time) as data_from,
        max(snapshot_time) as data_to,
        max(_batch_id) as latest_batch_id
    from {{ ref('stg_binance__open_interest_1h') }}

),

ratios as (

    select
        ratio_series || '_ratio_1h' as dataset,
        count(*) as row_count,
        count(distinct contract_symbol) as symbol_count,
        min(snapshot_time) as data_from,
        max(snapshot_time) as data_to,
        max(_batch_id) as latest_batch_id
    from {{ ref('stg_binance__long_short_ratio_1h') }}
    group by ratio_series

),

stats as (

    -- An aggregate without group by returns a row even on an empty table: count 0.
    select * from klines
    union all
    select * from funding
    union all
    select * from open_interest
    union all
    select * from ratios

)

select
    datasets.dataset,
    coalesce(stats.row_count, 0) as row_count,
    coalesce(stats.symbol_count, 0) as symbol_count,
    stats.data_from,
    stats.data_to,
    stats.latest_batch_id
from datasets
left join stats
    on datasets.dataset = stats.dataset
