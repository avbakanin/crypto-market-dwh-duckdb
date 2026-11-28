-- USD-M premium index per (contract, UTC day): the mean of the hourly closes, the close of
-- the day's last hour, the day's range, and the hours present. Hours without samples (a
-- delisted contract's idle tail) are left out. Live: the API tail has the hours of D-1
-- by the 00:07 run of D.

select
    symbol,
    open_time::date as trade_date,
    count(*) as hours_present,
    avg(close_premium::double) as premium_mean,
    arg_max(close_premium, open_time) as premium_close,
    min(low_premium) as premium_low,
    max(high_premium) as premium_high
from {{ ref('int_um__premium_index_1h') }}
where sample_count > 0
group by all
