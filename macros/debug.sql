{% macro metric(metric_name, grain, dimensions, secondary_calcs) %}
    {% if not execute %}
        {% do return("not execute") %}
    {% endif %}

    {% set metric = metrics.get_metric(metric_name) %}

    {%- set sql = metrics.get_metric_sql(
        metric,
        grain=grain,
        dims=dimensions,
        calcs=secondary_calcs
    ) %}
    ({{ sql }})
{% endmacro %}

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