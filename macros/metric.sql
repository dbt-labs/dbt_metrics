{% macro metric(metric_name, grain, dimensions=[], secondary_calculations=[]) -%}
    -- Need this here, since the actual ref is nested within loops/conditions:
    -- depends on: {{ ref('dbt_metrics_default_calendar') }}
    
    {%- if not execute %}
        {%- do return("not execute") %}
    {%- endif %}

    {%- set metric = metrics.get_metric(metric_name) %}

    {%- set sql = metrics.get_metric_sql(
        metric=metric,
        grain=grain,
        dimensions=dimensions,
        secondary_calculations=secondary_calculations
    ) %}
    ({{ sql }}) metric_subq
{%- endmacro %}

{% macro get_metric(metric_name) %}
    {% if not execute %}
        {% do return(None) %}
    {% else %}
    {% set metric_info = namespace(metric_id=none) %}
    {% for metric in graph.metrics.values() %}
        {% if metric.name == metric_name %}
            {% set metric_info.metric_id = metric.unique_id %}
        {% endif %}
    {% endfor %}

    {% if metric_info.metric_id is none %}
        {% do exceptions.raise_compiler_error("Metric named '" ~ metric_name ~ "' not found") %}
    {% endif %}
    

    {% do return(graph.metrics[metric_info.metric_id]) %}
    {% endif %}

{% endmacro %}