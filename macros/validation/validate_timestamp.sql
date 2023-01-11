{% macro validate_timestamp(grain, metric_tree, metrics_dictionary, dimensions) %}

    {# We check the metrics being used and if there is no grain we ensure that 
    none of the dimensions provided are from the calendar #}
    {% if not grain %}
        {%- if metrics.get_calendar_dimensions(dimensions) | length > 0 -%}

        {% for metric_name in metric_tree.full_set %}
            {% set metric_relation = metrics_dictionary[metric_name]%}
            {% if not metric_relation.timestamp %}
                {%- do exceptions.raise_compiler_error("The metric " ~ metric_name ~ " is using a calendar dimension but does not have a timestamp defined.") %}
            {% endif %}
        {% endfor %}

        {% endif %}
    {% endif %}

{% endmacro %}