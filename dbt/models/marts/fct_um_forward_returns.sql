-- Forward returns of the USD-M universe after costs: one row per (decision_date D, member
-- of D, horizon), long and short. docs/features_and_labels.md has the model; in short:
-- - entry: open of the hourly candle D + entry_delay_hours (the signal is ready ~00:15),
--   only if that candle traded (a member can be delisted between D-2 and D);
-- - exit: open of the candle entry + horizon days (the target), or of the next traded
--   candle within a day when the archive misses hours (BNXUSDT 2022-06); a contract with
--   no traded candle from the target to a day after it (delisted or halted: TLMUSDT came
--   back 9 months later) exits at the close of its last traded candle before the target
--   (exit_reason 'delisted': the loss of a delisting stays in the labels);
-- - funding: settlements in (entry, exit], paid by the long, received by the short, on
--   the entry notional;
-- - costs per side: the taker fee plus slippage max(floor, rv_30d * sqrt(order / average
--   daily turnover over 30 days)) in the features of D (square-root impact);
-- - MAE / MFE: the worst / best excursion of the traded candles from the entry to the
--   target, per side; survives_<x>x: the MAE stays above -(1/x - maintenance margin).
-- A horizon is labelled once the archive has every hourly candle to a day past its target,
-- so a contract the API tail no longer fetches is not taken for delisted.
-- One pass over the held candles with an aggregate per horizon (memory: 1 GiB).

{% set horizons = var('label_horizons') %}
{% set max_horizon = horizons | max %}

with hours as not materialized (

    select
        symbol,
        open_time,
        open_time::date as trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        trade_count,
        source
    from {{ ref('int_um__klines_1h') }}

),

labelled_through as (

    -- The newest hour the archive has: it publishes every downloaded contract daily.
    select max(open_time) as last_open from hours where source = 'archive'

),

entries as (

    select
        features.decision_date,
        features.symbol,
        features.rv_30d,
        features.avg_turnover_30d,
        hours.open_time as entry_time,
        hours.open_price as entry_price
    from {{ ref('int_um__features_daily') }} as features
    inner join hours
        on hours.symbol = features.symbol
        and hours.open_time
            = features.decision_date::timestamptz + to_hours({{ var('entry_delay_hours') }})
    where hours.trade_count > 0

),

held as (

    -- Traded candles from the entry to a day past the longest horizon, joined by day.
    select
        entries.decision_date,
        entries.symbol,
        entries.entry_time,
        hours.open_time,
        hours.open_price,
        hours.high_price,
        hours.low_price,
        hours.close_price
    from entries
    cross join range({{ max_horizon + 2 }}) as offsets(day_offset)
    inner join hours
        on hours.symbol = entries.symbol
        and hours.trade_date = entries.decision_date + offsets.day_offset::integer
    where hours.open_time >= entries.entry_time and hours.trade_count > 0

),

per_horizon as (

    select
        decision_date,
        symbol
        {% for h in horizons %}
        {% set target = 'entry_time + to_days(' ~ h ~ ')' %}
        {% set before = 'open_time < ' ~ target %}
        {% set next = 'open_time >= ' ~ target ~ ' and open_time < ' ~ target ~ ' + to_days(1)' %}
        , min(low_price) filter (where {{ before }}) as low_{{ h }}
        , max(high_price) filter (where {{ before }}) as high_{{ h }}
        , max(open_time) filter (where {{ before }}) as last_open_{{ h }}
        , arg_max(close_price, open_time) filter (where {{ before }}) as last_price_{{ h }}
        , min(open_time) filter (where {{ next }}) as next_open_{{ h }}
        , arg_min(open_price, open_time) filter (where {{ next }}) as next_price_{{ h }}
        {% endfor %}
    from held
    group by decision_date, symbol

),

exits as (

    {% for h in horizons %}
    select
        entries.*,
        {{ h }} as horizon_days,
        per_horizon.next_open_{{ h }} is null as is_delisted,
        coalesce(per_horizon.next_open_{{ h }}, per_horizon.last_open_{{ h }} + to_hours(1))
            as exit_time,
        coalesce(per_horizon.next_price_{{ h }}, per_horizon.last_price_{{ h }}) as exit_price,
        per_horizon.low_{{ h }} as path_low,
        per_horizon.high_{{ h }} as path_high
    from entries
    inner join per_horizon using (decision_date, symbol)
    where entries.entry_time + to_days({{ h + 1 }})
        <= (select last_open from labelled_through)
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

),

funding as (

    select
        exits.decision_date,
        exits.symbol,
        exits.horizon_days,
        sum(funding.funding_rate) as funding_sum,
        count(*) as funding_count
    from exits
    cross join range({{ max_horizon + 2 }}) as offsets(day_offset)
    inner join {{ ref('int_um__funding_rate') }} as funding
        on funding.contract_symbol = exits.symbol
        and funding.funding_hour::date = exits.decision_date + offsets.day_offset::integer
    where funding.funding_hour > exits.entry_time and funding.funding_hour <= exits.exit_time
    group by all

),

returns as (

    select
        exits.decision_date,
        exits.symbol,
        exits.horizon_days,
        exits.entry_time,
        exits.exit_time,
        case when exits.is_delisted then 'delisted' else 'horizon' end as exit_reason,
        exits.entry_price,
        exits.exit_price,
        (exits.exit_price / exits.entry_price - 1)::decimal(38, 12) as gross_return,
        coalesce(funding.funding_sum, 0)::decimal(38, 12) as funding_return,
        coalesce(funding.funding_count, 0) as funding_count,
        (
            2 * ({{ var('taker_fee_bps') }} + greatest(
                {{ var('slippage_floor_bps') }},
                coalesce(1e4 * exits.rv_30d
                    * sqrt({{ var('slippage_order_usd') }} / exits.avg_turnover_30d), 0)
            )) / 1e4
        )::decimal(38, 12) as cost_return,
        (exits.path_low / exits.entry_price - 1)::decimal(38, 12) as mae_long,
        (exits.path_high / exits.entry_price - 1)::decimal(38, 12) as mfe_long,
        (1 - exits.path_high / exits.entry_price)::decimal(38, 12) as mae_short,
        (1 - exits.path_low / exits.entry_price)::decimal(38, 12) as mfe_short
    from exits
    left join funding using (decision_date, symbol, horizon_days)

)

select
    *,
    gross_return - funding_return - cost_return as net_return_long,
    -gross_return + funding_return - cost_return as net_return_short,
    {% for x in var('leverage_levels') %}
    mae_long > -(1.0 / {{ x }} - {{ var('maintenance_margin_rate') }}) as long_survives_{{ x }}x,
    mae_short > -(1.0 / {{ x }} - {{ var('maintenance_margin_rate') }}) as short_survives_{{ x }}x
    {%- if not loop.last %},{% endif %}
    {% endfor %}
from returns
