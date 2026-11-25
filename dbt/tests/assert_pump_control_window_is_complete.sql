-- Every pump event has control_hours_per_event hours in each pool, each with all
-- 2 * pump_window_hours + 1 offsets, all of the event's own symbol: the profile's
-- control medians weigh every event the same. Returns the events that differ.

{%- set per_event = var('control_hours_per_event') %}
{%- set offsets = 2 * var('pump_window_hours') + 1 %}

with per_event as (

    select
        events.pump_event_id,
        count(distinct if(controls.is_year_control, controls.control_open_time_ms, null))
            as year_controls,
        count(distinct if(controls.is_recent_control, controls.control_open_time_ms, null))
            as recent_controls,
        count(distinct controls.control_open_time_ms) as controls,
        count(controls.pump_event_id) as window_rows,
        count_if(controls.symbol <> events.symbol) as foreign_rows
    from {{ ref('fct_pump_events') }} as events
    left join {{ ref('fct_pump_control_window') }} as controls
        on events.pump_event_id = controls.pump_event_id
    group by events.pump_event_id

)

select *
from per_event
where
    year_controls <> {{ per_event }}
    or recent_controls <> {{ per_event }}
    or window_rows <> controls * {{ offsets }}
    or foreign_rows > 0
