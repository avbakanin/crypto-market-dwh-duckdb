{#
  Missing funding settlements per contract: inside the series (the model's
  is_after_missed_settlement flag) and at its end (the last settlement that should exist
  as of the latest extract batch on the contract's interval grid, 00:00 UTC based).
  Never filled in: a missed settlement is a fact about the source (source note 35).

  publish_lag_minutes  settlements are expected that long after their hour
#}
{% test complete_funding_settlements(model, column_name, publish_lag_minutes=5) %}

with settlements as (

    select
        contract_symbol,
        {{ column_name }} as hour_ms,
        interval_hours,
        hours_since_previous,
        is_after_missed_settlement,
        _batch_id
    from {{ model }}

),

as_of as (

    -- strptime gives a timestamp without a zone: its epoch is the UTC wall clock of the id.
    select epoch_ms(strptime(max(_batch_id), '%Y%m%dT%H%M%SZ')) as as_of_ms
    from settlements

),

last_per_contract as (

    select contract_symbol, hour_ms as last_ms, interval_hours
    from settlements
    qualify row_number() over (partition by contract_symbol order by hour_ms desc) = 1

),

tail as (

    select
        last_per_contract.contract_symbol,
        last_per_contract.last_ms,
        (as_of.as_of_ms - {{ publish_lag_minutes }} * 60000)
        // (last_per_contract.interval_hours * 3600000)
        * last_per_contract.interval_hours * 3600000 as expected_last_ms
    from last_per_contract
    cross join as_of

)

select
    contract_symbol,
    epoch_ms(hour_ms) as settlement_hour_utc,
    'previous settlement missing: ' || hours_since_previous || ' h after the last one, '
    || 'interval ' || interval_hours || ' h' as issue
from settlements
where is_after_missed_settlement

union all

select
    contract_symbol,
    epoch_ms(expected_last_ms),
    'tail missing: last settlement at ' || epoch_ms(last_ms)::varchar
from tail
where last_ms < expected_last_ms

{% endtest %}
