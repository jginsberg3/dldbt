{#
  Installed by `dldbt install-macro`. When a dbt model runs under dldbt, the
  DLDBT_BRANCH env var holds the sanitized branch schema name. Every model
  lands in that schema unless it declares a custom_schema_name (in which case
  we suffix, matching dbt's default pattern of "<target>_<custom>").

  Falls back to target.schema if DLDBT_BRANCH is unset so plain `dbt run`
  still works outside dldbt.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set dldbt_branch = env_var('DLDBT_BRANCH', '') -%}
    {%- set base = dldbt_branch if dldbt_branch else target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ base }}
    {%- else -%}
        {{ base }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
