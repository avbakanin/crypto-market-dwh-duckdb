-- Forward-test results per registered version (seed signal_definitions, status
-- forward_test): logged days, trades, and the closed trades' net after costs and funding.
-- ci_low / ci_high: mean +- 1.96 standard errors of the closed trades, a first look that
-- ignores the overlap of 14-day holdings; the decision to drop the paper mark also runs the
-- research's block bootstrap (docs/forward_test.md). criteria_met: running at least
-- signal_criteria_min_days, at least signal_criteria_min_closed closed trades, ci_low > 0
-- (the same numbers as signals/registry.py).

with versions as (

    select version, horizon_days, decided_on
    from {{ ref('signal_definitions') }}
    where status = 'forward_test'

),

log as (

    select * from {{ ref('fct_signal_log') }}

),

per_version as (

    select
        version,
        min(decision_date) as first_decision_date,
        max(decision_date) as last_decision_date,
        count(*) as days_logged,
        count(*) filter (where status = 'missed') as days_missed,
        count(*) filter (where status = 'no_trade') as days_no_trade,
        count(*) filter (where status = 'trade') as trades,
        count(*) filter (where outcome_status = 'open') as open_trades,
        count(*) filter (where outcome_status = 'closed') as closed_trades,
        count(*) filter (where outcome_status = 'closed' and side = 'long') as closed_longs,
        count(*) filter (where is_stopped) as stopped_trades,
        avg((net_return > 0)::integer) filter (where outcome_status = 'closed') as hit_rate,
        avg(net_return::double) filter (where outcome_status = 'closed') as mean_net,
        stddev_samp(net_return::double) filter (where outcome_status = 'closed') as sd_net,
        min(net_return::double) filter (where outcome_status = 'closed') as worst_net,
        avg(excess_return::double) filter (where outcome_status = 'closed') as mean_excess,
        avg(survives_3x::integer) filter (where outcome_status = 'closed') as survive_3x_share,
        quantile_cont(mae::double, 0.1) filter (where outcome_status = 'closed') as mae_p10
    from log
    group by version

)

select
    versions.version,
    versions.horizon_days,
    versions.decided_on,
    per_version.* exclude (version),
    coalesce(per_version.last_decision_date - per_version.first_decision_date + 1, 0)
        as days_running,
    per_version.mean_net - 1.96 * per_version.sd_net / sqrt(per_version.closed_trades)
        as ci_low,
    per_version.mean_net + 1.96 * per_version.sd_net / sqrt(per_version.closed_trades)
        as ci_high,
    coalesce(
        per_version.last_decision_date - per_version.first_decision_date + 1
            >= {{ var('signal_criteria_min_days') }}
        and per_version.closed_trades >= {{ var('signal_criteria_min_closed') }}
        and per_version.mean_net - 1.96 * per_version.sd_net / sqrt(per_version.closed_trades)
            > 0,
        false
    ) as criteria_met
from versions
left join per_version using (version)
