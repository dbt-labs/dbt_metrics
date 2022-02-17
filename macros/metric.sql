{% macro metric(metric_name, grain, dimensions=[], secondary_calculations=[]) -%}
    -- Need this here, since the actual ref is nested within loops/conditions:
    -- depends on: {{ ref('dbt_metrics_default_calendar') }}
    
    {%- if not execute %}
        {%- do return("not execute") %}
    {%- endif %}

    {# use built-in context method #}
    {%- set metric = get_metric(metric_name) %}

    {%- set sql = metrics.get_metric_sql(
        metric=metric,
        grain=grain,
        dimensions=dimensions,
        secondary_calculations=secondary_calculations
    ) %}
    ({{ sql }}) metric_subq
{%- endmacro %}
