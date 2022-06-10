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