-- From the event to a signal: does hour t that looks like a pre-pump hour predict a pump
-- in hour t + 1? fct_pump_events looks back from pumps (P(signal | pump)); a trade needs
-- P(pump | signal) against the base rate of every hour. One row per (rule, UTC day of
-- hour t): hours where the rule held and how many of them were followed by an hour
-- rising past the pump threshold. Rule every_hour gives the base rate and all pumps.
-- Daily rows let an analysis resample days (events of one day move together).
-- Rules use only hour t and before: the volume baseline is the 7 days before t.
-- Hours count when they have a volume baseline and the next hour is exactly one hour
-- later (a hole in the grid has no "next hour").

{%- set threshold = var('pump_min_return') %}
{%- set rules = [
    ('every_hour', 'true'),
    ('volume_1_5x', 'volume_ratio >= 1.5'),
    ('volume_2x', 'volume_ratio >= 2'),
    ('volume_2x_flat_price', 'volume_ratio >= 2 and abs(return_1h) < 0.02'),
    ('volume_3x_rising_below_threshold',
        'volume_ratio >= 3 and return_1h >= 0 and return_1h < ' ~ threshold),
] %}

with hours as (

    select
        open_time_ms,
        return_1h,
        quote_volume / quote_volume_baseline as volume_ratio,
        lead(open_time_ms) over (partition by symbol order by open_time_ms)
            as next_open_time_ms,
        lead(return_1h) over (partition by symbol order by open_time_ms) as next_return_1h
    from {{ ref('int_binance__hourly_returns') }}

),

scored as (

    select
        -- epoch_ms(ms) is the UTC wall clock: the UTC day, whatever the session's time zone
        epoch_ms(open_time_ms)::date as signal_date,
        return_1h,
        volume_ratio,
        next_return_1h >= {{ threshold }} as is_followed_by_pump
    from hours
    where
        volume_ratio is not null
        and next_open_time_ms = open_time_ms + 3600000
        and next_return_1h is not null

)

{%- for name, condition in rules %}

select
    '{{ name }}' as rule,
    signal_date,
    count(*) as signal_hours,
    -- bigint: count_if is a HUGEINT in DuckDB, and Parquet would store it as a double.
    count_if(is_followed_by_pump)::bigint as followed_by_pump
from scored
where {{ condition }}
group by signal_date
{%- if not loop.last %}

union all
{%- endif %}
{%- endfor %}
