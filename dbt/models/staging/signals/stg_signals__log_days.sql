-- One row per file of the signal log (signals/journal.py): the day, its hash and the
-- previous day's hash it quotes. The chain is checked by assert_signal_log_chain_intact.

with files as (

    select
        regexp_extract(filename, '(\d{4}-\d{2}-\d{2})\.json$', 1)::date as file_date,
        sha256(content) as file_sha256,
        content::json as doc
    from {{ signal_log_files() }}

)

select
    file_date,
    (doc ->> 'decision_date')::date as decision_date,
    (doc ->> 'entry_time')::timestamptz as entry_time,
    (doc ->> 'logged_at')::timestamptz as logged_at,
    doc ->> 'prev_sha256' as prev_sha256,
    file_sha256,
    (doc ->> 'registered_on')::date as registered_on,
    json_array_length(doc -> 'decisions') as decision_count
from files
