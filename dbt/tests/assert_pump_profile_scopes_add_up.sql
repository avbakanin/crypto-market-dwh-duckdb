-- Every event is in 'all' and in exactly one of single_coin / multi_coin: per offset and
-- metric, the event counts of the two scopes add up to 'all'. Returns the rows that differ.

with counts as (

    select
        offset_hours,
        metric,
        sum(if(event_scope = 'all', events, 0)) as all_events,
        sum(if(event_scope <> 'all', events, 0)) as scoped_events
    from {{ ref('mart_pump_profile') }}
    group by offset_hours, metric

)

select *
from counts
where all_events <> scoped_events
