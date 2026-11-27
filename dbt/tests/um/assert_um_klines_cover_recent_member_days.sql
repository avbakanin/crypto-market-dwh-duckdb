{{ config(severity='error', alias='dq_um_klines_recent_member_gaps') }}

-- Stop: a universe member without 24 hourly candles on a day of the recent window. The
-- signal of the day reads these candles (archive, then the API tail). Known false alarm:
-- a member delisted on D-1 has no API candles that day (rare: a member traded on D-2).

{{ um_member_day_gaps(
    ref('int_um__klines_1h'), 'symbol', 'open_time',
    'current_date - ' ~ var('um_recent_days'), 'current_date - 1'
) }}
