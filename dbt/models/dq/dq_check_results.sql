-- One row per data test or unit test per dbt invocation, from the DQ log the on-run-end
-- hook appends to (macros/dq_log.sql). A view, not a table: the hook writes after every
-- node is built, so a table built in the same run would always be one invocation behind.
-- is_latest marks the last result of each check: per check, not per invocation, since a
-- selective run (dbt build --select staging) leaves the other checks where they were.

with results as (

    select *
    from {{ source('dq', 'dbt_node_results') }}
    where resource_type in ('test', 'unit_test')

)

select
    invocation_id,
    run_started_at,
    dbt_command,
    node_unique_id as check_unique_id,
    node_name as check_name,
    case
        when resource_type = 'unit_test' then 'unit'
        when generic_test is not null then 'generic'
        else 'singular'
    end as check_type,
    generic_test,
    -- The model a generic or unit test is attached to; a singular test may read several.
    coalesce(
        split_part(tested_node_unique_id, '.', -1),
        nullif(
            replace(regexp_replace(depends_on_nodes, '[^,.]+[.][^,.]+[.]', '', 'g'), ',', ', '),
            ''
        )
    ) as model_name,
    column_name,
    -- Unit tests have no severity: a failing one fails the build.
    lower(coalesce(severity, 'error')) as severity,
    status,
    failures,
    execution_time_s,
    failures_relation,
    message,
    completed_at,
    -- The warehouse's clock (current_timestamp of the hook's transaction), unlike the dbt
    -- process's run_started_at and completed_at.
    _logged_at as logged_at,
    row_number() over (
        partition by node_unique_id
        order by run_started_at desc, invocation_id desc
    ) = 1 as is_latest
from results
