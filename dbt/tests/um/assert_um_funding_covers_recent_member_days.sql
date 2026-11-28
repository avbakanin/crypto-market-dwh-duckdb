{{ config(severity='error', alias='dq_um_funding_recent_member_gaps') }}

-- Stop: a universe member without a funding row on a day of the recent window. Features
-- read the funding of D-1 and labels the funding over the holding period; the archive has
-- funding by month only, so the current month of every member comes from the API tail
-- (members of the last FUNDING_MEMBER_DAYS days, ingestion/universe.py). Days up to D-1
-- are checked for that day's members, D-1 also for today's members. Not for tomorrow's:
-- the universe of D+1 appears when the archive publishes D-1, and its newcomers reach the
-- API extract on the next run (source note 140).

with universe as (

    select universe_date, symbol, is_member
    from {{ ref('int_um__universe_daily') }}

),

members as (

    select universe_date as day, symbol
    from universe
    where is_member
        and universe_date >= current_date - {{ var('um_recent_days') }}
        and universe_date <= current_date - 1
    union
    select current_date - 1 as day, symbol
    from universe
    where is_member
        and universe_date = least(current_date, (select max(universe_date) from universe))

),

funding as (

    select contract_symbol as symbol, funding_hour::date as day, count(*) as rows_present
    from {{ ref('int_um__funding_rate') }}
    where funding_hour::date >= current_date - {{ var('um_recent_days') }}
    group by all

)

select members.day, members.symbol
from members
left join funding using (symbol, day)
where funding.symbol is null
