-- The signal log is append-only: every day's file quotes the previous day's hash, days run
-- without gaps, each file is named after its day, and nothing but a missed day is logged at
-- or after the day's entry (a later decision could see the entry price). signals/journal.py
-- checks the same before each run; this check covers the files the warehouse read.

with days as (

    select
        *,
        lag(file_sha256) over (order by decision_date) as previous_sha256,
        lag(decision_date) over (order by decision_date) as previous_date
    from {{ ref('stg_signals__log_days') }}

),

decided as (

    select decision_date, bool_or(status <> 'missed') as has_decision
    from {{ ref('stg_signals__decisions') }}
    group by decision_date

)

select days.*, decided.has_decision
from days
left join decided using (decision_date)
where days.file_date <> days.decision_date
    or days.prev_sha256 <> coalesce(days.previous_sha256, repeat('0', 64))
    or days.decision_date <> coalesce(days.previous_date + 1, days.decision_date)
    or (decided.has_decision and days.logged_at >= days.entry_time)
