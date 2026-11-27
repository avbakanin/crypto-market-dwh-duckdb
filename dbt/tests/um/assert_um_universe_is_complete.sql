{{ config(severity='error', alias='dq_um_universe_incomplete') }}

-- Stop: every day from the first universe day to the newest has members, as many as
-- min(um_universe_size, eligible contracts). A day without members means the 1d candles
-- of every contract are missing around it (a lost batch), which would shift the windows.

with days as (

    select range::date as universe_date
    from range(
        (select min(universe_date) from {{ ref('int_um__universe_daily') }}),
        (select max(universe_date) from {{ ref('int_um__universe_daily') }}) + 1,
        interval 1 day
    )

),

per_day as (

    select
        universe_date,
        count(*) as eligible,
        count(*) filter (where is_member) as members
    from {{ ref('int_um__universe_daily') }}
    group by universe_date

)

select days.universe_date, coalesce(eligible, 0) as eligible, coalesce(members, 0) as members
from days
left join per_day using (universe_date)
where coalesce(members, 0) = 0
    or members <> least(eligible, {{ var('um_universe_size') }})
