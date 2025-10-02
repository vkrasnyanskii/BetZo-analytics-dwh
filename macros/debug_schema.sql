ï»¿{% macro debug_schema() %}
  {% set resolved = generate_schema_name('bi', none) %}
  {% do log("DEBUG >> generate_schema_name('bi') = " ~ resolved, info=True) %}
{% endmacro %}

