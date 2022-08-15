{% macro metric(metric_name, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None) -%}
    {{ return(adapter.dispatch('metric', 'metrics')(metric_name, grain, dimensions, secondary_calculations, start_date, end_date, where)) }}
{% endmacro %}


{% macro default__metric(metric_name, grain, dimensions=[], secondary_calculations=[], start_date=None, end_date=None, where=None) -%}
    -- Need this here, since the actual ref is nested within loops/conditions:
    -- depends on: {{ ref(var('dbt_metrics_calendar_model', 'dbt_metrics_default_calendar')) }}

    {%- set error_message = "Warning: From v0.3.0 onwards, the metric macro has been renamed to calculate and the inputs changed. Use of the original metric macro is being phased out - please switch over to the new format." -%}
    {%- do exceptions.warn(error_message) -%}

    {%- if not execute %}
        {%- do return("not execute") %}
    {%- endif %}

    {% if metric_name is iterable and (metric_name is not string and metric_name is not mapping) %}
        {%- do exceptions.raise_compiler_error("The deprecated metric macro does not support multiple metrics. To use this functionality, please migrate to the calculate macro - more information on this in the README.") %}
    {% endif %}

    {%- set sql = metrics.get_metric_sql(
        metric_list=metric_name,
        grain=grain,
        dimensions=dimensions,
        secondary_calculations=secondary_calculations,
        start_date=start_date,
        end_date=end_date,
        where=where,
        macro='metric'
    ) %}
    ({{ sql }}) metric_subq
{%- endmacro %}
