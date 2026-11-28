-- Daily features of the USD-M universe: one row per (decision_date D, member of D). The
-- signal of D is computed at 00:07 UTC, so every feature reads days up to D-1 only (the
-- daily bar of D-1 closes at 00:00 of D; metrics: the snapshot of D-1 23:00). Windows are
-- trailing (macros/um_windows.sql); a feature whose window lacks days is null, as is a
-- metric before the archive has it (open interest from 2021-12, global long/short from
-- 2022-02, top-trader ratios from 2023-01: source note 138). A member without a bar on
-- D-1 keeps its row with null features. Unit test: future rows change nothing.

{% set return_days = [1, 3, 7, 14, 30, 60] %}

with bars as (

    select
        *,
        ln(close_price / {{ um_days_ago('close_price', 1) }}) as log_return_1d,
        greatest(
            high_price - low_price,
            abs(high_price - {{ um_days_ago('close_price', 1) }}),
            abs(low_price - {{ um_days_ago('close_price', 1) }})
        ) / close_price as true_range_pct
    from {{ ref('int_um__daily_bars') }}

),

rolling as (

    select
        symbol,
        trade_date,
        close_price,
        quote_volume,
        hours_present,
        {% for n in return_days %}
        close_price / {{ um_days_ago('close_price', n) }} - 1 as ret_{{ n }}d,
        {% endfor %}
        case when {{ um_trailing('count(*)', 20) }} = 20
            then close_price / {{ um_trailing('avg(close_price)', 20) }} - 1 end
            as close_to_sma20,
        case when {{ um_trailing('count(*)', 50) }} = 50
            then close_price / {{ um_trailing('avg(close_price)', 50) }} - 1 end
            as close_to_sma50,
        case when {{ um_trailing('count(realized_var)', 7) }} = 7
            then sqrt({{ um_trailing('avg(realized_var)', 7) }}) end as rv_7d,
        case when {{ um_trailing('count(realized_var)', 30) }} = 30
            then sqrt({{ um_trailing('avg(realized_var)', 30) }}) end as rv_30d,
        case when {{ um_trailing('count(true_range_pct)', 14) }} = 14
            then {{ um_trailing('avg(true_range_pct)', 14) }} end as atr_14d_pct,
        case when {{ um_trailing('count(*)', 30) }} = 30
            then {{ um_trailing('avg(quote_volume)', 30) }} end as avg_turnover_30d,
        case when {{ um_trailing('count(*)', 30) }} = 30
            then (ln(quote_volume) - {{ um_trailing('avg(ln(quote_volume))', 30) }})
                / nullif({{ um_trailing('stddev_samp(ln(quote_volume))', 30) }}, 0) end
            as turnover_z_30d,
        taker_buy_quote_volume / nullif(quote_volume, 0) as taker_buy_share_1d,
        case when {{ um_trailing('count(*)', 7) }} = 7
            then {{ um_trailing('sum(taker_buy_quote_volume)', 7) }}
                / nullif({{ um_trailing('sum(quote_volume)', 7) }}, 0) end
            as taker_buy_share_7d,
        funding_sum as funding_1d,
        case when {{ um_trailing('count(funding_sum)', 7) }} = 7
            then {{ um_trailing('sum(funding_sum)', 7) }} end as funding_7d,
        case when {{ um_trailing('count(funding_sum)', 30) }} = 30
            then (funding_sum - {{ um_trailing('avg(funding_sum)', 30) }})
                / nullif({{ um_trailing('stddev_samp(funding_sum)', 30) }}, 0) end
            as funding_z_30d,
        oi_usdt_eod as oi_usdt,
        oi_usdt_eod / {{ um_days_ago('oi_usdt_eod', 1) }} - 1 as oi_chg_1d,
        oi_usdt_eod / {{ um_days_ago('oi_usdt_eod', 7) }} - 1 as oi_chg_7d,
        case when {{ um_trailing('count(*)', 7) }} = 7
            then oi_usdt_eod / nullif({{ um_trailing('avg(quote_volume)', 7) }}, 0) end
            as oi_to_turnover_7d,
        global_ls,
        global_ls - {{ um_days_ago('global_ls', 7) }} as global_ls_chg_7d,
        top_position_ls,
        top_account_ls,
        log_return_1d,
        count(*) over (
            partition by symbol order by trade_date
            rows between unbounded preceding and current row
        ) as history_days
    from bars

),

btc as (

    select
        trade_date,
        log_return_1d as btc_log_return_1d,
        ret_7d as btc_ret_7d,
        ret_30d as btc_ret_30d,
        close_to_sma50 as btc_close_to_sma50
    from rolling
    where symbol = 'BTCUSDT'

),

relative as (

    select
        rolling.*,
        btc.btc_ret_7d,
        btc.btc_ret_30d,
        btc.btc_close_to_sma50,
        rolling.ret_7d - btc.btc_ret_7d as rel_btc_ret_7d,
        rolling.ret_30d - btc.btc_ret_30d as rel_btc_ret_30d,
        case when {{ um_trailing('regr_count(log_return_1d, btc_log_return_1d)', 30) }} = 30
            then {{ um_trailing('regr_slope(log_return_1d, btc_log_return_1d)', 30) }} end
            as beta_btc_30d
    from rolling
    left join btc using (trade_date)

),

members as (

    select universe_date as decision_date, symbol, turnover_rank
    from {{ ref('int_um__universe_daily') }}
    where is_member

),

joined as (

    select
        members.decision_date,
        members.symbol,
        members.turnover_rank,
        relative.* exclude (symbol, trade_date, close_price, quote_volume, log_return_1d),
        relative.trade_date as feature_date
    from members
    left join relative
        on relative.symbol = members.symbol
        and relative.trade_date = members.decision_date - 1

)

select
    * exclude (hours_present, history_days),
    case when ret_7d is not null then percent_rank() over (
        partition by decision_date, ret_7d is null order by ret_7d
    ) end as ret_7d_rank_pct,
    case when ret_30d is not null then percent_rank() over (
        partition by decision_date, ret_30d is null order by ret_30d
    ) end as ret_30d_rank_pct,
    avg((close_to_sma20 > 0)::integer) over (partition by decision_date)
        as breadth_above_sma20,
    median(ret_7d) over (partition by decision_date) as breadth_ret_7d_median,
    coalesce(hours_present, 0) as hours_feature_day,
    coalesce(history_days, 0) as history_days
from joined
