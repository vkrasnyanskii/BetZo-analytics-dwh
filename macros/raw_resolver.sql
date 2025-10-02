{% macro raw(table_name) -%}
  {{ source('betzo_raw', table_name) }}
{%- endmacro %}
