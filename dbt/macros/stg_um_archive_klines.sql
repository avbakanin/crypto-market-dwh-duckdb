{#
    Staging of archive klines (1d or 1h): typed, UTC, one row per (symbol, open_time).
    Incremental: only batches newer than the newest already here are read (the loader
    refuses a batch older than one loaded, ingestion/datasets.py), and a key they bring
    replaces the row it had (delete+insert): the latest batch wins, as in a full rebuild.
    Archive files hold closed candles only; a monthly file repeats the daily ones.
#}
{% macro stg_um_archive_klines(table_name) %}

with source as (

    select * from {{ source('binance', table_name) }}
    {% if is_incremental() %}
    where _batch_id > (select max(_batch_id) from {{ this }})
    {% endif %}

)

select
    symbol,
    epoch_ms((payload->>'open_time')::bigint)::timestamptz as open_time,
    epoch_ms((payload->>'close_time')::bigint)::timestamptz as close_time,
    (payload->>'open')::decimal(38, 8) as open_price,
    (payload->>'high')::decimal(38, 8) as high_price,
    (payload->>'low')::decimal(38, 8) as low_price,
    (payload->>'close')::decimal(38, 8) as close_price,
    (payload->>'volume')::decimal(38, 8) as base_volume,
    (payload->>'quote_volume')::decimal(38, 8) as quote_volume,
    (payload->>'count')::bigint as trade_count,
    (payload->>'taker_buy_volume')::decimal(38, 8) as taker_buy_base_volume,
    (payload->>'taker_buy_quote_volume')::decimal(38, 8) as taker_buy_quote_volume,
    (payload->>'open_time')::bigint as open_time_ms,
    (payload->>'close_time')::bigint as close_time_ms,
    _batch_id,
    _loaded_at
from source
qualify row_number() over (
    partition by symbol, (payload->>'open_time')::bigint
    order by _batch_id desc, _file_row_number desc
) = 1

{% endmacro %}
