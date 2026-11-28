{{ config(severity='warn', store_failures_as='table', alias='dq_um_premium_member_gaps') }}

-- Warn: member days without 24 hourly premium index candles (archive, then the API tail).

{{ um_member_day_gaps(
    ref('int_um__premium_index_1h'), 'symbol', 'open_time',
    "date '2020-01-01'", 'current_date - 1'
) }}
