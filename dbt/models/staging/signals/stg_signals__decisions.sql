-- One row per (decision day, forward version) of the signal log: a trade, no trade or a
-- missed day, as logged before the day's entry.

with files as (

    select content::json as doc
    from {{ signal_log_files() }}

),

decisions as (

    select
        (doc ->> 'decision_date')::date as decision_date,
        (doc ->> 'logged_at')::timestamptz as logged_at,
        unnest(cast(doc -> 'decisions' as json[])) as d
    from files

)

select
    decision_date,
    d ->> 'version' as version,
    d ->> 'status' as status,
    d ->> 'reason' as reason,
    d ->> 'symbol' as symbol,
    d ->> 'side' as side,
    (d ->> 'score')::double as score,
    (d ->> 'horizon_days')::integer as horizon_days,
    (d ->> 'exit_time')::timestamptz as planned_exit_time,
    (d ->> 'stop_loss')::decimal(38, 12) as stop_loss,
    d ->> 'hedge_symbol' as hedge_symbol,
    d ->> 'hedge_side' as hedge_side,
    (d ->> 'hedge_ratio')::decimal(38, 12) as hedge_ratio,
    d ->> 'model_sha256' as model_sha256,
    (d ->> 'model_block')::date as model_block,
    (d ->> 'open_positions_before')::integer as open_positions_before,
    logged_at
from decisions
