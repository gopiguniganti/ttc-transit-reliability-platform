{#
  dbt's default behavior prefixes a model's custom schema with the target
  schema (e.g. "staging" -> "staging_staging"). This project wants the
  schema config in dbt_project.yml (staging/marts) to be the literal
  Postgres schema name, so override the default macro to just use it.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
