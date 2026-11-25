{{ config(tags=['dq_log']) }}

-- The DQ log against the findings tables. Anchor: the last full build, a `dbt build` in
-- which every model of the project succeeded, started at most dq_log_max_build_age_minutes
-- before this check (both times from the dbt process: a hook that stopped writing leaves
-- an older anchor whose counts would still match). For every test that stores its failing
-- rows as a table: the table exists, and its row count equals the failures of the
-- test's last executed run since that build (a later partial build or dbt test replaces
-- the table and logs the new count). A hook that stopped logging tests leaves no such run
-- and is a finding for every table (DuckDB keeps no creation time to compare with).
-- Runs after the build has written the log, as a second step:
--   dbt test --select tag:dq_log
-- (selectors.yml leaves it out of a plain dbt build: tests of a build run before its
-- on-run-end hook, so it would compare new tables with the previous run's log.)

{% set stored = [] %}
{% set models = [] %}
{% if execute %}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'test' and node.config.store_failures_as == 'table' %}
            {% set relation = adapter.get_relation(
                database=node.database, schema=node.schema, identifier=node.alias) %}
            {% do stored.append([node.unique_id, node.alias, relation]) %}
        {% elif node.resource_type == 'model' and node.package_name == project_name %}
            {% do models.append(node.unique_id) %}
        {% endif %}
    {% endfor %}
{% endif %}

with stored_tests as (

    {% for unique_id, table_name, relation in stored %}
    select
        '{{ unique_id }}' as check_unique_id,
        '{{ table_name }}' as table_name,
        {{ 'true' if relation else 'false' }} as table_exists,
        {% if relation %}(select count(*) from {{ relation }}){% else %}null::bigint{% endif %}
            as stored_rows
    {% if not loop.last %}union all{% endif %}
    {% else %}
    select
        null::varchar as check_unique_id,
        null::varchar as table_name,
        null::boolean as table_exists,
        null::bigint as stored_rows
    where false
    {% endfor %}

),

last_full_build as (

    select invocation_id, run_started_at
    from {{ source('dq', 'dbt_node_results') }}
    where dbt_command = 'build'
        and resource_type = 'model'
        and status = 'success'
        and node_unique_id in (
            {%- for unique_id in models %}
            '{{ unique_id }}'{{ ',' if not loop.last }}
            {%- else %}
            null
            {%- endfor %}
        )
        and run_started_at >= strptime(
            '{{ run_started_at.strftime('%Y-%m-%d %H:%M:%S.%f') }} +0000',
            '%Y-%m-%d %H:%M:%S.%f %z'
        ) - interval ({{ var('dq_log_max_build_age_minutes') }}) minute
    group by invocation_id, run_started_at
    having count(distinct node_unique_id) = {{ models | length }}
    order by run_started_at desc, invocation_id desc
    limit 1

),

-- Skipped and errored runs leave the table of the last run that executed the test.
last_executed as (

    select results.check_unique_id, results.status, results.failures, results.invocation_id
    from {{ ref('dq_check_results') }} as results
    inner join last_full_build
        on results.run_started_at >= last_full_build.run_started_at
    where results.status in ('pass', 'warn', 'fail')
    qualify row_number() over (
        partition by results.check_unique_id
        order by results.run_started_at desc, results.invocation_id desc
    ) = 1

)

select
    stored_tests.check_unique_id,
    stored_tests.table_name,
    stored_tests.stored_rows,
    (select invocation_id from last_full_build) as full_build_invocation_id,
    last_executed.invocation_id as logged_invocation_id,
    last_executed.status as logged_status,
    last_executed.failures as logged_failures,
    case
        when not stored_tests.table_exists then 'findings table missing'
        when not exists (select 1 from last_full_build)
            then 'no full build in the last {{ var('dq_log_max_build_age_minutes') }} minutes'
        when last_executed.check_unique_id is null then 'test not logged since the full build'
        else 'row count differs from the log'
    end as issue
from stored_tests
left join last_executed
    on stored_tests.check_unique_id = last_executed.check_unique_id
where not stored_tests.table_exists
    or last_executed.check_unique_id is null
    or last_executed.failures is distinct from stored_tests.stored_rows
