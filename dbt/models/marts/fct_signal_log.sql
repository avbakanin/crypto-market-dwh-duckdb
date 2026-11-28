-- The forward test's decisions with their outcomes: one row per (decision day, version).
-- docs/forward_test.md. A trade is closed once fct_um_forward_returns labels its horizon
-- for the pick and for the hedge (the archive is ~2 days behind), with the same entry,
-- exit, funding and costs as the research:
-- - pick: the side logged; hedge: hedge_ratio x the opposite BTCUSDT trade, per unit of
--   the pick's notional (exposure 1 + ratio); MAE: the sum of the legs' MAEs (their worst
--   hours need not coincide: conservative);
-- - stop (versions with stop_loss): the first traded hour of the pick, from the entry to
--   its exit, whose low (long) or high (short) crosses entry x (1 -/+ stop) closes both
--   legs: the pick at the stop price or at that hour's open when it gapped through, the
--   hedge at that hour's close; funding of both legs up to the hour's open; costs as in
--   the labels (a stop-market order is a taker order). The pick's MAE is then its exit;
--   the hedge keeps its MAE over the whole horizon (conservative).
-- - excess: the pick's net over the average net of the day's members on the same side
--   (the research's random-member baseline).

with decisions as (

    select * from {{ ref('stg_signals__decisions') }}

),

labels as (

    select * from {{ ref('fct_um_forward_returns') }}

),

trades as (

    select * from decisions where status = 'trade'

),

legs as (

    select
        trades.decision_date,
        trades.version,
        pick.entry_time,
        pick.exit_time as horizon_exit_time,
        pick.exit_reason as horizon_exit_reason,
        pick.entry_price,
        pick.cost_return as pick_cost,
        case when trades.side = 'long' then pick.net_return_long else pick.net_return_short end
            as pick_net_horizon,
        case when trades.side = 'long' then pick.mae_long else pick.mae_short end
            as pick_mae_horizon,
        hedge.entry_price as hedge_entry_price,
        hedge.cost_return as hedge_cost,
        case when trades.hedge_side = 'long' then hedge.net_return_long
            else hedge.net_return_short end as hedge_net_horizon,
        case when trades.hedge_side = 'long' then hedge.mae_long else hedge.mae_short end
            as hedge_mae,
        case when trades.side = 'long' then base.base_long else base.base_short end
            as base_return
    from trades
    inner join labels as pick
        on pick.decision_date = trades.decision_date
        and pick.symbol = trades.symbol
        and pick.horizon_days = trades.horizon_days
    left join labels as hedge
        on hedge.decision_date = trades.decision_date
        and hedge.symbol = trades.hedge_symbol
        and hedge.horizon_days = trades.horizon_days
    inner join (
        select
            decision_date,
            horizon_days,
            avg(net_return_long) as base_long,
            avg(net_return_short) as base_short
        from labels
        group by all
    ) as base
        on base.decision_date = trades.decision_date
        and base.horizon_days = trades.horizon_days
    -- Closed only when every leg is labelled.
    where trades.hedge_symbol is null or hedge.decision_date is not null

),

stop_hits as (

    select
        trades.decision_date,
        trades.version,
        min(hours.open_time) as stop_open_time
    from trades
    inner join legs using (decision_date, version)
    inner join {{ ref('int_um__klines_1h') }} as hours
        on hours.symbol = trades.symbol
        and hours.open_time >= legs.entry_time
        and hours.open_time < legs.horizon_exit_time
    where trades.stop_loss is not null
        and hours.trade_count > 0
        and (
            (trades.side = 'long'
                and hours.low_price <= legs.entry_price * (1 - trades.stop_loss))
            or (trades.side = 'short'
                and hours.high_price >= legs.entry_price * (1 + trades.stop_loss))
        )
    group by all

),

stops as (

    select
        stop_hits.decision_date,
        stop_hits.version,
        stop_hits.stop_open_time,
        case
            when trades.side = 'long'
                then least(pick_hour.open_price, legs.entry_price * (1 - trades.stop_loss))
            else greatest(pick_hour.open_price, legs.entry_price * (1 + trades.stop_loss))
        end as pick_exit_price,
        hedge_hour.close_price as hedge_exit_price
    from stop_hits
    inner join trades using (decision_date, version)
    inner join legs using (decision_date, version)
    inner join {{ ref('int_um__klines_1h') }} as pick_hour
        on pick_hour.symbol = trades.symbol
        and pick_hour.open_time = stop_hits.stop_open_time
    left join {{ ref('int_um__klines_1h') }} as hedge_hour
        on hedge_hour.symbol = trades.hedge_symbol
        and hedge_hour.open_time = stop_hits.stop_open_time

),

stop_funding as (

    select
        stops.decision_date,
        stops.version,
        coalesce(sum(funding.funding_rate) filter (where funding.contract_symbol = trades.symbol), 0)
            as pick_funding,
        coalesce(sum(funding.funding_rate) filter (
            where funding.contract_symbol = trades.hedge_symbol), 0) as hedge_funding
    from stops
    inner join trades using (decision_date, version)
    inner join legs using (decision_date, version)
    left join {{ ref('int_um__funding_rate') }} as funding
        on funding.contract_symbol in (trades.symbol, trades.hedge_symbol)
        and funding.funding_hour > legs.entry_time
        and funding.funding_hour <= stops.stop_open_time
    group by all

),

outcomes as (

    select
        legs.decision_date,
        legs.version,
        legs.base_return,
        stops.decision_date is not null as is_stopped,
        coalesce(stops.stop_open_time + to_hours(1), legs.horizon_exit_time) as exit_time,
        case when stops.decision_date is not null then 'stop'
            else legs.horizon_exit_reason end as exit_reason,
        case
            when stops.decision_date is null then legs.pick_net_horizon
            when trades.side = 'long'
                then stops.pick_exit_price / legs.entry_price - 1
                    - stop_funding.pick_funding - legs.pick_cost
            else 1 - stops.pick_exit_price / legs.entry_price
                + stop_funding.pick_funding - legs.pick_cost
        end as pick_net,
        case
            when stops.decision_date is null then legs.pick_mae_horizon
            when trades.side = 'long' then stops.pick_exit_price / legs.entry_price - 1
            else 1 - stops.pick_exit_price / legs.entry_price
        end as pick_mae,
        case
            when trades.hedge_symbol is null then 0
            when stops.decision_date is null then legs.hedge_net_horizon
            when trades.hedge_side = 'long'
                then stops.hedge_exit_price / legs.hedge_entry_price - 1
                    - stop_funding.hedge_funding - legs.hedge_cost
            else 1 - stops.hedge_exit_price / legs.hedge_entry_price
                + stop_funding.hedge_funding - legs.hedge_cost
        end as hedge_net,
        coalesce(legs.hedge_mae, 0) as hedge_mae,
        coalesce(trades.hedge_ratio, 0) as hedge_ratio
    from legs
    inner join trades using (decision_date, version)
    left join stops using (decision_date, version)
    left join stop_funding using (decision_date, version)

),

scored as (

    select
        *,
        (pick_net + hedge_ratio * hedge_net)::decimal(38, 12) as net_return,
        (pick_mae + hedge_ratio * hedge_mae)::decimal(38, 12) as mae,
        (1 + hedge_ratio)::decimal(38, 12) as exposure,
        (pick_net - base_return)::decimal(38, 12) as excess_return
    from outcomes

)

select
    decisions.decision_date,
    decisions.version,
    decisions.status,
    decisions.reason,
    decisions.symbol,
    decisions.side,
    decisions.score,
    decisions.horizon_days,
    decisions.hedge_symbol,
    decisions.hedge_side,
    decisions.hedge_ratio,
    decisions.stop_loss,
    decisions.model_sha256,
    decisions.model_block,
    decisions.logged_at,
    decisions.planned_exit_time,
    case
        when decisions.status <> 'trade' then null
        when scored.decision_date is null then 'open'
        else 'closed'
    end as outcome_status,
    scored.exit_time,
    scored.exit_reason,
    scored.is_stopped,
    scored.pick_net::decimal(38, 12) as pick_net_return,
    scored.hedge_net::decimal(38, 12) as hedge_net_return,
    scored.net_return,
    scored.mae,
    scored.exposure,
    scored.mae > -scored.exposure * (1.0 / 3 - {{ var('maintenance_margin_rate') }})
        as survives_3x,
    scored.base_return::decimal(38, 12) as base_return,
    scored.excess_return
from decisions
left join scored using (decision_date, version)
