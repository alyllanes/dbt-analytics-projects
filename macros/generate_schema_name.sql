{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set my_custom_schema = env_var('DBT_SCHEMA', 'default_dataset') -%}
    {{ my_custom_schema }}
{%- endmacro %}