{#
  The most frequent funding rate of a contract is its base rate per settlement:
  interest_rate_8h * interval_hours / 8, paid while the premium stays in the clamp band
  (source note 39). The base rate differs by contract (0 for BNBUSDT, source note 41),
  so each contract is compared with its own. A mismatch means a wrong interval, a stale
  interest rate in the perpetuals seed, or a market that left the band for good.
#}
{% test funding_mode_matches_base_rate(model, column_name) %}

with per_contract as (

    select
        contract_symbol,
        mode({{ column_name }}) as modal_rate,
        mode(interval_hours) as modal_interval_hours,
        any_value(interest_rate_8h) as interest_rate_8h,
        count(*) as settlements
    from {{ model }}
    group by contract_symbol

)

select
    contract_symbol,
    modal_rate,
    interest_rate_8h * modal_interval_hours / 8 as base_rate,
    modal_interval_hours,
    settlements
from per_contract
-- Compared times 8, not divided: decimal * integer stays exact in every engine, while
-- decimal / integer is a DOUBLE in DuckDB.
where modal_rate * 8 is distinct from interest_rate_8h * modal_interval_hours

{% endtest %}
