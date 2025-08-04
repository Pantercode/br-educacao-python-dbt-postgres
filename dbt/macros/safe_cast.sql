{% macro safe_cast_numeric(expr) -%}
  case
    when {{ expr }} ~ '^[+-]?[0-9]+(\.[0-9]+)?$' then ({{ expr }})::numeric
    else null
  end
{%- endmacro %}

{% macro safe_cast_int(expr) -%}
  case
    when {{ expr }} ~ '^[+-]?[0-9]+$' then ({{ expr }})::bigint
    else null
  end
{%- endmacro %}

{% macro safe_cast_date(expr, fmt='YYYY-MM-DD') -%}
  to_date({{ expr }}, '{{ fmt }}')
{%- endmacro %}
