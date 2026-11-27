-- Point-in-time universe of USD-M perpetuals: for each day D, every eligible contract
-- ranked by its quote turnover over the 30 closed days D-31 .. D-2. D-1 is left out on
-- purpose: the archive publishes a day only the morning after, and the signal of day D is
-- decided at 00:07 UTC. Eligible on D:
-- - all 30 days of the window present, and trades on D-2 (a delisted contract's last
--   candles are idle, trade_count = 0, source note 132);
-- - not pegged to the dollar: closes of the window all within [0.98, 1.02] (stablecoins);
-- - not a TradFi or index contract by the newest exchangeInfo snapshot (stocks, metals,
--   oil: contractType TRADIFI_PERPETUAL), and not in the exclusion seed (commodity tokens,
--   index contracts gone from exchangeInfo, known stablecoins).
-- Members: turnover_rank <= var('um_universe_size'). Delisted contracts stay in the past
-- universe (no survivorship bias); only the past is read (unit test with future rows).

with daily as (

    select
        symbol,
        open_time::date as trade_date,
        quote_volume,
        close_price,
        trade_count
    from {{ ref('stg_binance__um_archive_klines_1d') }}

),

windowed as (

    select
        symbol,
        trade_date + 2 as universe_date,
        sum(quote_volume) over last_30_days as turnover_30d,
        count(*) over last_30_days as days_in_window,
        min(close_price) over last_30_days as min_close_30d,
        max(close_price) over last_30_days as max_close_30d,
        trade_count as window_end_trade_count
    from daily
    window last_30_days as (
        partition by symbol
        order by trade_date
        range between interval 29 days preceding and current row
    )

),

eligible as (

    select *
    from windowed
    where
        days_in_window = 30
        and window_end_trade_count > 0
        and not (min_close_30d >= 0.98 and max_close_30d <= 1.02)
        and symbol not in (select symbol from {{ ref('um_universe_exclusions') }})
        and symbol not in (
            select contract_symbol
            from {{ ref('stg_binance__um_contracts') }}
            where contract_type = 'TRADIFI_PERPETUAL' or underlying_type = 'INDEX'
        )

),

ranked as (

    select
        universe_date,
        symbol,
        turnover_30d,
        row_number() over (
            partition by universe_date order by turnover_30d desc, symbol
        ) as turnover_rank
    from eligible

)

select
    universe_date,
    symbol,
    turnover_30d,
    turnover_rank,
    turnover_rank <= {{ var('um_universe_size') }} as is_member
from ranked
