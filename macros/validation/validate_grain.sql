{% macro validate_grain(grain, all_metric_names, base_metric_names) %}

    {# We loop through the full set here to ensure that the provided grain works for all metrics
    returned or used, not just those listed #}

    {% for metric_name in all_metric_names %}
        {% set metric_relation = metric(metric_name)%}
        {% if grain not in metric_relation.time_grains%}
            {% if metric_name not in base_metric_names %}
                {%- do exceptions.raise_compiler_error("The metric " ~ metric_name ~ " is an upstream metric of one of the provided metrics. The grain " ~ grain ~ " is not defined in its metric definition.") %}
            {% else %}
                {%- do exceptions.raise_compiler_error("The metric " ~ metric_name ~ " does not have the provided time grain " ~ grain ~ " defined in the metric definition.") %}
            {% endif %}
        {% endif %}
    {% endfor %}

{% endmacro %}