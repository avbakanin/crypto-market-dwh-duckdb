{{ config(severity='error', alias='dq_um_label_gaps') }}

-- Stop: a member that traded at its entry hour (D + entry_delay_hours) has no label for
-- a horizon the archive has covered (to a day past the target). A missing label drops
-- the row from every backtest, and drops would not be random (gaps, delistings).

with members as (

    select decision_date, symbol
    from {{ ref('int_um__features_daily') }}

),

hours as (

    select symbol, open_time, trade_count, source
    from {{ ref('int_um__klines_1h') }}

),

labelled_through as (

    select max(open_time) as last_open from hours where source = 'archive'

),

expected as (

    select members.decision_date, members.symbol, horizons.horizon_days
    from members
    inner join hours
        on hours.symbol = members.symbol
        and hours.open_time
            = members.decision_date::timestamptz + to_hours({{ var('entry_delay_hours') }})
    cross join (select unnest({{ var('label_horizons') }}) as horizon_days) as horizons
    where hours.trade_count > 0
        and hours.open_time + to_days(horizons.horizon_days + 1)
            <= (select last_open from labelled_through)

)

select expected.*
from expected
left join {{ ref('fct_um_forward_returns') }} as labels
    using (decision_date, symbol, horizon_days)
where labels.symbol is null
