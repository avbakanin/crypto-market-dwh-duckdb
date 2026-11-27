{{ config(severity='warn', store_failures_as='table', alias='dq_um_funding_archive_vs_api') }}

-- Warn: a settlement both the archive and the API have, at a different rate.

select
    a.contract_symbol,
    a.funding_hour,
    a.funding_rate,
    p.funding_rate as api_funding_rate
from {{ ref('stg_binance__um_archive_funding_rate') }} as a
inner join {{ ref('stg_binance__um_api_funding_rate') }} as p
    using (contract_symbol, funding_hour_ms)
where a.funding_rate <> p.funding_rate
