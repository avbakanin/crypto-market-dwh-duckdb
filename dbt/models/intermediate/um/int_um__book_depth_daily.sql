-- Order book depth of USD-M contracts per UTC day: the median over the day's hourly
-- snapshots of the USDT notional within 0.2 %, 1 % and 2 % of the mid, per side, and of
-- the 1 % imbalance (bids - asks) / (bids + asks). Archive only, from 2023-01-01: day D-1
-- is published in the morning of D, so a live feature of D can read D-2 at the latest.

with hourly as (

    select
        contract_symbol,
        snapshot_hour,
        max(notional_usdt) filter (where percentage = -0.20) as bid_02,
        max(notional_usdt) filter (where percentage = 0.20) as ask_02,
        max(notional_usdt) filter (where percentage = -1.00) as bid_1,
        max(notional_usdt) filter (where percentage = 1.00) as ask_1,
        max(notional_usdt) filter (where percentage = -2.00) as bid_2,
        max(notional_usdt) filter (where percentage = 2.00) as ask_2
    from {{ ref('stg_binance__um_archive_book_depth_1h') }}
    group by all

)

select
    contract_symbol,
    snapshot_hour::date as trade_date,
    count(*) as hours_present,
    median(bid_02::double) as bid_depth_02pct_usdt,
    median(ask_02::double) as ask_depth_02pct_usdt,
    median(bid_1::double) as bid_depth_1pct_usdt,
    median(ask_1::double) as ask_depth_1pct_usdt,
    median(bid_2::double) as bid_depth_2pct_usdt,
    median(ask_2::double) as ask_depth_2pct_usdt,
    median((bid_1 - ask_1)::double / nullif(bid_1 + ask_1, 0)::double) as imbalance_1pct
from hourly
group by all
