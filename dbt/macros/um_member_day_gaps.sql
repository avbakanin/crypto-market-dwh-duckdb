{#
    Days a universe member lacks hourly rows in a model: (day, symbol, hours present) for
    member days in [first_day, last_day] with fewer than 24 rows. Days up to D-2 are
    checked for that day's members (the archive has them by the morning of D-1); D-1 only
    for the members of the newest universe day, whose hours the API extract fetches. Idle
    candles of a delisted contract are rows like any other.
#}
{% macro um_member_day_gaps(relation, symbol_column, time_column, first_day, last_day) %}

with universe as (

    select universe_date, symbol, is_member
    from {{ ref('int_um__universe_daily') }}

),

members as (

    select universe_date as day, symbol
    from universe
    where is_member
        and universe_date >= {{ first_day }}
        and universe_date <= least({{ last_day }}, current_date - 2)
    union
    select current_date - 1 as day, symbol
    from universe
    where is_member
        and universe_date = (select max(universe_date) from universe)
        and current_date - 1 between {{ first_day }} and {{ last_day }}

),

hours as (

    select
        {{ symbol_column }} as symbol,
        {{ time_column }}::date as day,
        count(*) as hours_present
    from {{ relation }}
    where {{ time_column }}::date >= {{ first_day }}
    group by all

)

select members.day, members.symbol, coalesce(hours.hours_present, 0) as hours_present
from members
left join hours using (symbol, day)
where coalesce(hours.hours_present, 0) < 24

{% endmacro %}
