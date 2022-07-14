{% macro validate_grain(grain, full_set) %}

    {# We loop through the full set here to ensure that the provided grain works for all metrics
    returned or used, not just those listed #}

    {% for metric_name in full_set %}
        {% set metric_relation = metric(metric_name)%}
        {% if grain not in metric_relation.time_grains%}
            {%- do exceptions.raise_compiler_error("The metric " ~ metric_name ~ " does not have the provided time grain " ~ grain ~ "defined in the metric definition.") %}
        {% endif %}
    {% endfor %}

{% endmacro %}