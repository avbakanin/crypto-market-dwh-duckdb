{{ config(severity='warn', store_failures_as='table', alias='dq_um_premium_archive_vs_api') }}

-- Warn: an hour both the archive and the API have, with any value different. Both serve
-- the same closed premium index candles (equal on 2026-11-27, source note 143): a
-- difference is a train/serve skew.

select
    a.symbol,
    a.open_time,
    a.open_premium, p.open_premium as api_open_premium,
    a.high_premium, p.high_premium as api_high_premium,
    a.low_premium, p.low_premium as api_low_premium,
    a.close_premium, p.close_premium as api_close_premium,
    a.sample_count, p.sample_count as api_sample_count
from {{ ref('stg_binance__um_archive_premium_index_1h') }} as a
inner join {{ ref('stg_binance__um_api_premium_index_1h') }} as p using (symbol, open_time_ms)
where a.open_premium <> p.open_premium
    or a.high_premium <> p.high_premium
    or a.low_premium <> p.low_premium
    or a.close_premium <> p.close_premium
    or a.sample_count <> p.sample_count
