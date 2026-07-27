{#
  Cross-database macro overrides for DataFusion's Postgres surface.

  dbt's default__ implementations of dateadd/datediff emit Snowflake-style
  function calls (`dateadd('day', 5, x)`, `datediff('day', a, b)`) that
  DataFusion does not have. These rebuild them from constructs it does have:
  string-built interval casts (dynamic-safe: `x + cast(concat(n, ' days') as
  interval)`) and integer date subtraction (`date - date` -> whole days).
#}

{% macro hotdata__dateadd(datepart, interval, from_date_or_timestamp) -%}
  {{ from_date_or_timestamp }} + cast(concat({{ interval }}, ' {{ datepart }}') as interval)
{%- endmacro %}

{% macro hotdata__convert_timezone(column, target_tz, source_tz) -%}
  {#- dbt_date dispatch hook: DataFusion supports the Postgres double
      AT TIME ZONE pattern, so delegate to it. -#}
  cast(
    cast({{ column }} as timestamp)
    at time zone '{{ source_tz }}' at time zone '{{ target_tz }}'
    as timestamp
  )
{%- endmacro %}

{% macro hotdata__datediff(first_date, second_date, datepart) -%}
  {%- set datepart = datepart.lower() -%}
  {%- if datepart == 'day' -%}
    (cast({{ second_date }} as date) - cast({{ first_date }} as date))
  {%- elif datepart == 'week' -%}
    ((cast({{ second_date }} as date) - cast({{ first_date }} as date)) / 7)
  {%- elif datepart == 'month' -%}
    ((date_part('year', {{ second_date }}) - date_part('year', {{ first_date }})) * 12
      + (date_part('month', {{ second_date }}) - date_part('month', {{ first_date }})))
  {%- elif datepart == 'quarter' -%}
    ((date_part('year', {{ second_date }}) - date_part('year', {{ first_date }})) * 4
      + (date_part('quarter', {{ second_date }}) - date_part('quarter', {{ first_date }})))
  {%- elif datepart == 'year' -%}
    (date_part('year', {{ second_date }}) - date_part('year', {{ first_date }}))
  {%- elif datepart in ('hour', 'minute', 'second') -%}
    {%- set seconds_per = {'hour': 3600, 'minute': 60, 'second': 1}[datepart] -%}
    cast(floor((to_unixtime(cast({{ second_date }} as timestamp))
      - to_unixtime(cast({{ first_date }} as timestamp))) / {{ seconds_per }}) as bigint)
  {%- else -%}
    {% do exceptions.raise_compiler_error("datediff datepart '" ~ datepart ~ "' is not supported on Hotdata") %}
  {%- endif -%}
{%- endmacro %}
