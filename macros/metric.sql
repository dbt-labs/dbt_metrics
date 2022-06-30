{% macro metric(metrics, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where = []) -%}
    {{ return(adapter.dispatch('metric', 'metrics')(metrics, grain, dimensions, secondary_calculations, start_date, end_date, where)) }}
{% endmacro %}


{% macro default__metric(metrics, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where = []) -%}
    -- Need this here, since the actual ref is nested within loops/conditions:
    -- depends on: {{ ref(var('dbt_metrics_calendar_model', 'dbt_metrics_default_calendar')) }}

    {%- if not execute %}
        {%- do return("not execute") %}
    {%- endif %}

    {%- set sql = metrics.get_metric_sql(
        metrics=metrics,
        grain=grain,
        dimensions=dimensions,
        secondary_calculations=secondary_calculations,
        start_date=start_date,
        end_date=end_date,
        where=where
    ) %}
    ({{ sql }}) metric_subq
{%- endmacro %}
