-- Daily bars of the USD-M universe (UTC days) from the hourly series: the inputs of the
-- daily features. One row per (symbol, trade_date) with at least one hourly candle.
-- - price and volume: OHLC, quote and taker buy volume, trades, hours present;
-- - realized_var: sum of squared hourly log returns of the day (the first hour against
--   the previous day's last close), the base of realized volatility;
-- - funding_sum: the funding settlements stamped within the day;
-- - metrics: open interest at the day's last snapshot (hour 23: stamped before midnight,
--   so it is known when the signal of the next day is computed), long/short ratios as
--   the day's average of hourly snapshots.

with hours as (

    select
        symbol,
        open_time,
        open_time::date as trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        quote_volume,
        taker_buy_quote_volume,
        trade_count,
        ln(close_price / lag(close_price) over (partition by symbol order by open_time))
            as log_return
    from {{ ref('int_um__klines_1h') }}

),

bars as (

    select
        symbol,
        trade_date,
        arg_min(open_price, open_time) as open_price,
        max(high_price) as high_price,
        min(low_price) as low_price,
        arg_max(close_price, open_time) as close_price,
        sum(quote_volume) as quote_volume,
        sum(taker_buy_quote_volume) as taker_buy_quote_volume,
        sum(trade_count) as trade_count,
        count(*) as hours_present,
        sum(log_return * log_return) as realized_var
    from hours
    group by symbol, trade_date

),

funding as (

    select
        contract_symbol as symbol,
        funding_hour::date as trade_date,
        sum(funding_rate) as funding_sum,
        count(*) as funding_count
    from {{ ref('int_um__funding_rate') }}
    group by all

),

metrics as (

    select
        contract_symbol as symbol,
        snapshot_time::date as trade_date,
        arg_max(open_interest_usdt, snapshot_time) as oi_usdt_eod,
        max(snapshot_time) as oi_snapshot_time,
        avg(global_account_ratio) as global_ls,
        avg(top_position_ratio) as top_position_ls,
        avg(top_account_ratio) as top_account_ls
    from {{ ref('int_um__metrics_1h') }}
    group by all

)

select
    bars.*,
    funding.funding_sum,
    coalesce(funding.funding_count, 0) as funding_count,
    metrics.oi_usdt_eod,
    metrics.oi_snapshot_time,
    metrics.global_ls,
    metrics.top_position_ls,
    metrics.top_account_ls
from bars
left join funding using (symbol, trade_date)
left join metrics using (symbol, trade_date)
