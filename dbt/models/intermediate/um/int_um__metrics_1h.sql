-- USD-M hourly open interest and long/short ratios of the universe: the archive where it
-- has the hour (exact ratios), the API for the hours after (ratios from rounded shares,
-- note 130). One row per (contract, hour); the taker ratio comes with the archive only
-- (the klines' taker volume is the full series). snapshot_lag_minutes: 5 where the
-- archive's value is the snapshot 5 minutes past the hour (before 2026-11-16, note 137).

with archive as (

    select
        contract_symbol, snapshot_time, open_interest, open_interest_usdt,
        top_account_ratio, top_position_ratio, global_account_ratio,
        taker_buy_sell_ratio, snapshot_lag_minutes, snapshot_time_ms
    from {{ ref('stg_binance__um_archive_metrics_1h') }}

),

api as (

    select
        contract_symbol, snapshot_time, open_interest, open_interest_usdt,
        top_account_ratio, top_position_ratio, global_account_ratio,
        null::decimal(38, 12) as taker_buy_sell_ratio, 0 as snapshot_lag_minutes,
        snapshot_time_ms
    from {{ ref('stg_binance__um_api_metrics_1h') }} as a
    where not exists (
        select 1 from archive
        where archive.contract_symbol = a.contract_symbol
            and archive.snapshot_time_ms = a.snapshot_time_ms
    )

)

select *, 'archive' as source from archive
union all
select *, 'api' as source from api
