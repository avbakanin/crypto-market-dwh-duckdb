{#
  Rows per complete UTC day = (rows in series_model) x points_per_day. Returns the days
  that differ, including days with no rows at all.

  Catches what per-series checks cannot: a whole series missing (the grid test checks
  each series it sees; relationships only check child -> parent), and fan-out of a join
  that duplicated rows.

  Complete days: from the first day the latest-starting series covers in full to the
  last day that ends at or before the latest point.

  column_name     the point time in Unix ms
  partition_by    the series key
  series_model    the list of series that must all be present (dim_symbol)
  points_per_day  24 for hourly series
  max_days        calendar size; a longer range returns a row saying so
#}
{% test complete_daily_row_count(model, column_name, partition_by, series_model, points_per_day=24, max_days=2000) %}

with points as (

    select {{ partition_by }} as series, {{ column_name }} as point_ms
    from {{ model }}

),

series_bounds as (

    select series, min(point_ms) as first_ms, max(point_ms) as last_ms
    from points
    group by series

),

bounds as (

    select
        -- Integer arithmetic: / would be a DOUBLE in DuckDB (source note 109).
        (max(first_ms) + 86400000 - 1) // 86400000 * 86400000 as first_day_ms,
        -- A day is complete once its last point (day + 23 h) is in.
        (max(last_ms) + 3600000) // 86400000 * 86400000 as end_day_ms
    from series_bounds

),

calendar as (

    -- range() is dense: 0, 1, ..., max_days - 1.
    select bounds.first_day_ms + calendar_days.range * 86400000 as day_ms
    from range({{ max_days }}) as calendar_days
    cross join bounds

),

days as (

    select calendar.day_ms
    from calendar
    cross join bounds
    where calendar.day_ms < bounds.end_day_ms

),

counted as (

    select point_ms // 86400000 * 86400000 as day_ms, count(*) as row_count
    from points
    group by 1

),

expected as (

    select count(*) * {{ points_per_day }} as rows_per_day
    from {{ series_model }}

)

select
    -- epoch_ms(ms) is a timestamp without a zone: the UTC wall clock.
    epoch_ms(days.day_ms)::date as day_utc,
    coalesce(counted.row_count, 0) as row_count,
    expected.rows_per_day
from days
cross join expected
left join counted
    on days.day_ms = counted.day_ms
where coalesce(counted.row_count, 0) <> expected.rows_per_day

union all

select
    epoch_ms(first_day_ms)::date,
    null,
    null
from bounds
where (end_day_ms - first_day_ms) // 86400000 > {{ max_days }}

{% endtest %}
