{{ config(severity='warn', store_failures_as='table', alias='dq_um_book_depth_member_gaps') }}

-- Warn: member days from 2023 (the archive's first book depth) up to D-2 (archive only,
-- no API tail) without a snapshot in each of the 24 hours.

{% set hours %}
    (select distinct contract_symbol, snapshot_hour
     from {{ ref('stg_binance__um_archive_book_depth_1h') }})
{% endset %}

{{ um_member_day_gaps(
    hours, 'contract_symbol', 'snapshot_hour', "date '2023-01-01'", 'current_date - 2'
) }}
