{#
  The signal log files (data/signals/log/*.json, written by signals/run.py) as a relation
  (filename, content). Before the first file exists read_text would fail on the empty
  glob: then an empty relation of the same shape.
#}
{% macro signal_log_files() %}
    {%- set glob = var('signals_log_glob') -%}
    {%- set found = 0 -%}
    {%- if execute -%}
        {%- set found = run_query("select count(*) from glob('" ~ glob ~ "')").columns[0].values()[0] -%}
    {%- endif -%}
    {%- if found > 0 -%}
        (select filename, content from read_text('{{ glob }}'))
    {%- else -%}
        (select null::varchar as filename, null::varchar as content where false)
    {%- endif -%}
{% endmacro %}
