-- USD-M hourly premium index of the universe: the archive where it has the hour, the API
-- for the hours after. One row per (symbol, open_time); `source` says which. Where both
-- have an hour they must agree (tests/um/assert_um_premium_archive_matches_api.sql).

with archive as (

    select
        symbol, open_time, open_premium, high_premium, low_premium, close_premium,
        sample_count, open_time_ms
    from {{ ref('stg_binance__um_archive_premium_index_1h') }}

),

api as (

    select
        symbol, open_time, open_premium, high_premium, low_premium, close_premium,
        sample_count, open_time_ms
    from {{ ref('stg_binance__um_api_premium_index_1h') }} as a
    where not exists (
        select 1 from archive
        where archive.symbol = a.symbol and archive.open_time_ms = a.open_time_ms
    )

)

select *, 'archive' as source from archive
union all
select *, 'api' as source from api
