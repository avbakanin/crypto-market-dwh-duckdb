-- USD-M funding settlements of the universe: the archive (monthly files, up to the last
-- full month) where it has the settlement, the API for the ones after. One row per
-- (contract, settlement hour); interval_hours comes with the archive only.

with archive as (

    select contract_symbol, funding_hour, funding_rate, interval_hours, funding_hour_ms
    from {{ ref('stg_binance__um_archive_funding_rate') }}

),

api as (

    select
        contract_symbol,
        funding_hour,
        funding_rate,
        null::integer as interval_hours,
        funding_hour_ms
    from {{ ref('stg_binance__um_api_funding_rate') }} as a
    where not exists (
        select 1 from archive
        where archive.contract_symbol = a.contract_symbol
            and archive.funding_hour_ms = a.funding_hour_ms
    )

)

select *, 'archive' as source from archive
union all
select *, 'api' as source from api
