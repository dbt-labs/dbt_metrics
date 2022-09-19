{% macro validate_grain(grain, metric_tree, metrics_dictionary, secondary_calculations) %}

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

    {% if grain == 'all_time' %}

        {% if secondary_calculations | length > 0 %}
            {%- do exceptions.raise_compiler_error("The selected grain - all_time - does not support secondary calculations.") %}
        {% endif %}

        {% for metric_name in metric_tree.full_set %}
            {% if metrics_dictionary[metric_name].window is not none%}
                {%- do exceptions.raise_compiler_error("The selected grain - all_time - does not support metrics with window definitions.") %}
            {% endif%}
        {% endfor%}

    {% endif %}

{% endmacro %}