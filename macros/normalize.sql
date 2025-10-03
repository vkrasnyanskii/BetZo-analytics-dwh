{# приведение статуса к верхнему регистру #}
{% macro normalize_bet_status(col) -%}
  upper(toString({{ col }}))
{%- endmacro %}

{# классификация игрового события по статусу #}
{% macro classify_bet_status(col) -%}
  case
    when upper(toString({{ col }})) in ('BET') then 'STAKE'
    when upper(toString({{ col }})) in ('WIN') then 'WIN'
    when upper(toString({{ col }})) in ('LOSE') then 'LOSS'
    when upper(toString({{ col }})) in ('ROLLBACK') then 'ROLLBACK'
    else 'OTHER'
  end
{%- endmacro %}