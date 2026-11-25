-- DQ metrics of the DuckDB warehouse, as the dq branches of
-- docs/baseline/snowflake_baseline.sql compute them in Snowflake: same relations, metric
-- names and grains. The findings tables are counted one by one (Snowflake read the
-- metadata row_count of INFORMATION_SCHEMA.TABLES; DuckDB keeps only an estimate): a
-- missing table fails the query instead of dropping out. The journal's own history
-- (rows, invocations, first and last run) is snowflake_only and not computed here.
-- The check counts (reference) describe the last build and the last dbt test in the
-- journal: build, then dbt test --select tag:dq_log, on the baseline's input.
-- Run by analysis/compare_baseline.py --layer dq.

with last_invocations as (
    select dbt_command, arg_max(invocation_id, run_started_at) as invocation_id
    from dq.dq_check_results
    where dbt_command in ('build', 'test')
    group by dbt_command
),

last_invocation_results as (
    select results.*
    from dq.dq_check_results as results
    inner join last_invocations using (dbt_command, invocation_id)
)

select 'dq' as layer, 'dq_data_coverage' as relation, dataset as grain, metric, value
from (
    select
        dataset,
        row_count::varchar as row_count,
        symbol_count::varchar as symbol_count,
        strftime(data_from, '%Y-%m-%dT%H:%M:%SZ') as data_from_utc,
        strftime(data_to, '%Y-%m-%dT%H:%M:%SZ') as data_to_utc,
        latest_batch_id
    from dq.dq_data_coverage
) unpivot (value for metric in (row_count, symbol_count, data_from_utc, data_to_utc, latest_batch_id))

union all

select 'dq', 'dq_check_results (last build and last test)',
    dbt_command || ' ' || check_type || ' ' || severity || ' ' || status, metric, value
from (
    select
        dbt_command, check_type, severity, status,
        count(*)::varchar as checks,
        coalesce(sum(failures), 0)::varchar as sum_failures
    from last_invocation_results
    group by all
) unpivot (value for metric in (checks, sum_failures))

union all

select 'dq', 'dq_check_results (last build and last test)', check_name, metric, value
from (
    select
        check_name,
        max(status) as status,
        max(failures)::varchar as failures
    from last_invocation_results
    where lower(status) <> 'pass'
    group by check_name
) unpivot (value for metric in (status, failures))

union all

select 'dq', 'findings tables (store_failures_as table)', table_name, 'row_count', row_count::varchar
from (
    select 'dq_funding_missing_settlements' as table_name, count(*) as row_count from dq.dq_funding_missing_settlements
    union all select 'dq_funding_mode_vs_base_rate', count(*) from dq.dq_funding_mode_vs_base_rate
    union all select 'dq_funding_revised_history', count(*) from dq.dq_funding_revised_history
    union all select 'dq_funding_time_offset', count(*) from dq.dq_funding_time_offset
    union all select 'dq_klines_missing_hours', count(*) from dq.dq_klines_missing_hours
    union all select 'dq_klines_revised_history', count(*) from dq.dq_klines_revised_history
    union all select 'dq_long_short_missing_hours', count(*) from dq.dq_long_short_missing_hours
    union all select 'dq_long_short_revised_history', count(*) from dq.dq_long_short_revised_history
    union all select 'dq_market_daily_row_count', count(*) from dq.dq_market_daily_row_count
    union all select 'dq_open_interest_missing_hours', count(*) from dq.dq_open_interest_missing_hours
    union all select 'dq_open_interest_revised_history', count(*) from dq.dq_open_interest_revised_history
    union all select 'dq_spot_futures_divergence', count(*) from dq.dq_spot_futures_divergence
    union all select 'dq_volume_anomaly', count(*) from dq.dq_volume_anomaly
)
