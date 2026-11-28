{{ config(severity='error', alias='dq_um_features_core_nulls') }}

-- Stop: a member with a full feature day (24 hours on D-1) and at least 31 days of bars
-- lacks a core price/volume feature. Their windows are 30 days at most; a NULL here
-- means a gap in the hourly history or a broken window, and a signal would read it.

{% set core = [
    'ret_1d', 'ret_7d', 'ret_30d', 'close_to_sma20', 'rv_7d', 'rv_30d', 'atr_14d_pct',
    'avg_turnover_30d', 'turnover_z_30d', 'taker_buy_share_7d', 'beta_btc_30d'
] %}

select decision_date, symbol
    {% for column in core %}, {{ column }} is null as {{ column }}_missing{% endfor %}
from {{ ref('int_um__features_daily') }}
where history_days >= 31
    and hours_feature_day = 24
    and ({% for column in core %}{{ column }} is null{% if not loop.last %} or {% endif %}{% endfor %})
