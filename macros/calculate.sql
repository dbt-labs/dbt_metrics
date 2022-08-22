{% macro calculate(metric_list, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None, allow_calendar_dimensions=False) -%}
    {{ return(adapter.dispatch('calculate', 'metrics')(metric_list, grain, dimensions, secondary_calculations, start_date, end_date, where, allow_calendar_dimensions)) }}
{% endmacro %}


{% macro default__calculate(metric_list, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None, allow_calendar_dimensions=False) -%}
    -- Need this here, since the actual ref is nested within loops/conditions:
    -- depends on: {{ ref(var('dbt_metrics_calendar_model', 'dbt_metrics_default_calendar')) }}

    {%- if not execute %}
        {%- do return("not execute") %}
    {%- endif %}

    {%- set sql = metrics.get_metric_sql(
        metric_list=metric_list,
        grain=grain,
        dimensions=dimensions,
        secondary_calculations=secondary_calculations,
        start_date=start_date,
        end_date=end_date,
        where=where,
        initiated_by='calculate',
        allow_calendar_dimensions=allow_calendar_dimensions
    ) %}
    ({{ sql }}) metric_subq
{%- endmacro %}
