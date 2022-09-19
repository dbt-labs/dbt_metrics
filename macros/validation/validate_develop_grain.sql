{% macro validate_develop_grain(grain, metric_tree, metrics_dictionary, secondary_calculations) %}

    {# We loop through the full set here to ensure that the provided grain works for all metrics
    returned or used, not just those listed #}

    {% if grain == 'all_time' %}

        {% if secondary_calculations | length > 0 %}
            {%- do exceptions.raise_compiler_error("The selected grain - all_time - does not support secondary calculations.") %}
        {% endif %}

        {% for metric in metrics_dictionary %}
            {% if metric.window is not none %}
                {%- do exceptions.raise_compiler_error("The selected grain - all_time - does not support metrics with window definitions.") %}
            {% endif %}
        {% endfor%}

    {% endif %}

{% endmacro %}