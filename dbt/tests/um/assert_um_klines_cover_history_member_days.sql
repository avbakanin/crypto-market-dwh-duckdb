{{ config(severity='warn', store_failures_as='table', alias='dq_um_klines_history_member_gaps') }}

-- Warn: member days before the recent window without 24 hourly candles: the archive's own
-- gaps (exchange outages), kept as they are; features must handle a missing hour.

{{ um_member_day_gaps(
    ref('int_um__klines_1h'), 'symbol', 'open_time',
    "date '2020-01-01'", 'current_date - 1 - ' ~ var('um_recent_days')
) }}
