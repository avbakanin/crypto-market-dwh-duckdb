{#
    Staging of archive klines (1d or 1h): typed, UTC, one row per (symbol, open_time).
    Incremental: only batches newer than the newest already here are read (the loader
    refuses a batch older than one loaded, ingestion/datasets.py), and a key they bring
    replaces the row it had (delete+insert): the latest batch wins, as in a full rebuild.
    Archive files hold closed candles only; a monthly file repeats the daily ones.
    Times: Unix ms; spot files stamp microseconds from 2025 (source note 143), taken to ms.
#}
{% macro stg_um_archive_klines(table_name) %}

{%- set open_ms = archive_ms("payload->>'open_time'") -%}
{%- set close_ms = archive_ms("payload->>'close_time'") %}

with source as (

    select * from {{ source('binance', table_name) }}
    {% if is_incremental() %}
    where _batch_id > (select max(_batch_id) from {{ this }})
    {% endif %}

)

select
    symbol,
    epoch_ms({{ open_ms }})::timestamptz as open_time,
    epoch_ms({{ close_ms }})::timestamptz as close_time,
    (payload->>'open')::decimal(38, 8) as open_price,
    (payload->>'high')::decimal(38, 8) as high_price,
    (payload->>'low')::decimal(38, 8) as low_price,
    (payload->>'close')::decimal(38, 8) as close_price,
    (payload->>'volume')::decimal(38, 8) as base_volume,
    (payload->>'quote_volume')::decimal(38, 8) as quote_volume,
    (payload->>'count')::bigint as trade_count,
    (payload->>'taker_buy_volume')::decimal(38, 8) as taker_buy_base_volume,
    (payload->>'taker_buy_quote_volume')::decimal(38, 8) as taker_buy_quote_volume,
    {{ open_ms }} as open_time_ms,
    {{ close_ms }} as close_time_ms,
    _batch_id,
    _loaded_at
from source
qualify row_number() over (
    partition by symbol, {{ open_ms }}
    order by _batch_id desc, _file_row_number desc
) = 1

{% endmacro %}


{# Unix ms of an archive time field: microseconds (>= 1e14, note 143) are divided by 1000.
   Must match the loader's key_sql (ingestion/datasets.py). #}
{% macro archive_ms(field) -%}
    (case when ({{ field }})::bigint >= 100000000000000
        then ({{ field }})::bigint // 1000 else ({{ field }})::bigint end)
{%- endmacro %}
