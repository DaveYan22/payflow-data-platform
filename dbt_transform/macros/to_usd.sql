{#- Convert to USD with a units-per-USD rate. A missing amount or rate stays NULL, never 0. -#}
{% macro to_usd(amount, rate) -%}
  CAST(CAST({{ amount }} AS Nullable(Decimal(38, 6))) / {{ rate }} AS Nullable(Decimal(18, 6)))
{%- endmacro %}
