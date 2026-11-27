{{ config(severity='warn', store_failures_as='table', alias='dq_um_funding_interval_gaps') }}

-- Warn: archive settlements whose next one is neither interval_hours later nor the next
-- one's interval later (the interval changing): a skipped settlement or a missing month.

with settlements as (

    select
        contract_symbol,
        funding_hour,
        interval_hours,
        lead(funding_hour) over (
            partition by contract_symbol order by funding_hour
        ) as next_funding_hour,
        lead(interval_hours) over (
            partition by contract_symbol order by funding_hour
        ) as next_interval_hours
    from {{ ref('stg_binance__um_archive_funding_rate') }}

)

select *
from settlements
where next_funding_hour is not null
    and next_funding_hour - funding_hour
        not in (to_hours(interval_hours), to_hours(next_interval_hours))
