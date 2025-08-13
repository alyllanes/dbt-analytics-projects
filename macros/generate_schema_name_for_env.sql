{% macro generate_schema_name_for_env(custom_schema_name, node) -%}
    {% if target.name == 'prod' %}
        {{ custom_schema_name or 'production_dataset' }}
    {% else %}
        {{ custom_schema_name or 'dev_' ~ target.name }}
    {% endif %}
{%- endmacro %}
