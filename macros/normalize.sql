{% macro norm_ts_i32_to_dt64(col) -%}
  toDateTime64({{ col }}, 3, ''UTC'')
{%- endmacro %}

{% macro norm_upper(col) -%}
  upper({{ col }})
{%- endmacro %}

{% macro norm_money_minor_to_decimal(minor_col) -%}
  toDecimal128({{ minor_col }} / 100.0, 2)
{%- endmacro %}
