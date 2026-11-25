{#
  Build models in the schema their config names (staging, marts, dq) as is, instead of
  dbt's default "<target schema>_<custom schema>" (staging_staging). The project has one
  environment in one file; a team would give each developer a schema (or a file).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim | lower }}
    {%- endif -%}
{%- endmacro %}
