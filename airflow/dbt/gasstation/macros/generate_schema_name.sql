-- Override dbt's default schema naming to use the custom schema name directly,
-- without prepending the target schema from profiles.yml.
-- Without this macro, dbt generates "staging_staging" and "staging_mart".
-- With this macro: staging models → "staging", mart models → "mart".
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
