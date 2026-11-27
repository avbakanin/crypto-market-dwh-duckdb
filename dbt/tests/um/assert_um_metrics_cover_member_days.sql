{{ config(severity='warn', store_failures_as='table', alias='dq_um_metrics_member_gaps') }}

-- Warn: member days without 24 hourly open interest / long-short rows, from 2021-12-01:
-- before it the archive has metrics files for a few contracts only (source note 136).
-- The 5-minute grid behind each hour is in the manifests (rows_5m, duplicates_5m,
-- unordered_5m, missing_5m, missing_hours).

{{ um_member_day_gaps(
    ref('int_um__metrics_1h'), 'contract_symbol', 'snapshot_time',
    "date '2021-12-01'", 'current_date - 1'
) }}
