{#
    Daily windows of the features over int_um__daily_bars: by symbol, ordered by trade_date,
    RANGE frames on the date, so a missing day stays missing (never the neighbour's row).
    Every frame ends at the current row: a feature reads its own day and earlier only.
#}

{# The value exactly n days before the row's day; null when that day has no bar. #}
{% macro um_days_ago(expr, n, partition='symbol') -%}
    first_value({{ expr }}) over (
        partition by {{ partition }} order by trade_date
        range between interval {{ n }} days preceding and interval {{ n }} days preceding
    )
{%- endmacro %}

{# An aggregate over the n days ending at the row's day (n bars at most). #}
{% macro um_trailing(agg, n, partition='symbol') -%}
    {{ agg }} over (
        partition by {{ partition }} order by trade_date
        range between interval {{ n - 1 }} days preceding and current row
    )
{%- endmacro %}
