{{ config(severity='warn', store_failures_as='table', alias='dq_um_metrics_archive_vs_api') }}

-- Warn: an hour both the archive and the API have, with open interest different, or a
-- long/short ratio unlike the archive's exact one rounded as the API rounds it (note 137):
-- the position ratio to 4 decimals; an account ratio from the long share rounded to 4
-- (note 130; at a ratio of 4.8 that is 1.7e-3 off the exact one), within 5e-4 (a share on
-- a rounding edge: 11 of 70k hours, 3e-4). Archive hours stamped 5 minutes early (before
-- 2026-11-16, note 137) hold another snapshot and are left out.

{% set ratios = ['top_account_ratio', 'top_position_ratio', 'global_account_ratio'] %}

with archive as (

    select
        contract_symbol,
        snapshot_time_ms,
        snapshot_time,
        open_interest,
        open_interest_usdt,
        {% for r in ratios %}
        {{ r }},
        round({{ r }} / (1 + {{ r }}), 4) as {{ r }}_share,
        {% endfor %}
    from {{ ref('stg_binance__um_archive_metrics_1h') }}
    where snapshot_lag_minutes = 0

)

select
    a.contract_symbol,
    a.snapshot_time,
    a.open_interest, p.open_interest as api_open_interest,
    a.open_interest_usdt, p.open_interest_usdt as api_open_interest_usdt,
    {% for r in ratios %}
    a.{{ r }}, p.{{ r }} as api_{{ r }},
    {% endfor %}
from archive as a
inner join {{ ref('stg_binance__um_api_metrics_1h') }} as p
    using (contract_symbol, snapshot_time_ms)
where a.open_interest <> p.open_interest
    or a.open_interest_usdt <> p.open_interest_usdt
    or abs(round(a.top_position_ratio, 4) - p.top_position_ratio) > 0.00011
    {% for r in ['top_account_ratio', 'global_account_ratio'] %}
    or abs(round(a.{{ r }}_share / (1 - a.{{ r }}_share), 4) - p.{{ r }}) > 0.0005
    {% endfor %}
