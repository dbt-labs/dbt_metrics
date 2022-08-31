{% macro validate_grain(grain, metric_tree) %}

    {# We loop through the full set here to ensure that the provided grain works for all metrics
    returned or used, not just those listed #}

    {% for metric_name in metric_tree.full_set %}
        {% set metric_relation = metric(metric_name)%}
        {% if grain not in metric_relation.time_grains%}
            {% if metric_name not in metric_tree.base_set %}
                {%- do exceptions.raise_compiler_error("The metric " ~ metric_name ~ " is an upstream metric of one of the provided metrics. The grain " ~ grain ~ " is not defined in its metric definition.") %}
            {% else %}
                {%- do exceptions.raise_compiler_error("The metric " ~ metric_name ~ " does not have the provided time grain " ~ grain ~ " defined in the metric definition.") %}
            {% endif %}
        {% endif %}
    {% endfor %}

{% endmacro %}