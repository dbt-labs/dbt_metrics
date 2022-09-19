{% macro validate_dimension_list(dimensions, metric_tree) %}
    
    {# This macro exists to invalidate dimensions provided to the metric macro that are not viable 
    candidates based on metric definitions. This prevents downstream run issues when the sql 
    logic attempts to group by provided dimensions and fails because they don't exist for 
    one or more of the required metrics. #}

    {% set calendar_dimensions = var('custom_calendar_dimension_list',[]) %}

    {% for dim in dimensions %}

        {# Now we loop through all the metrics in the full set, which is all metrics, parent metrics,
        and derived metrics associated with the macro call #}
        {% for metric_name in metric_tree.full_set %}
            {% set metric_relation = metric(metric_name)%}
            
            {# This macro returns a list of dimensions that are inclusive of calendar dimensions #}
            {% set complete_dimension_list = metric_relation.dimensions + calendar_dimensions %}

            {# If the dimension provided is not present in the loop metrics dimension list then we 
            will raise an error. If it is missing in ANY of the metrics, it cannot be used in the 
            macro call. Only dimensions that are valid in all metrics are valid in the macro call #}
            {% if dim not in complete_dimension_list %}
                {% if dim not in calendar_dimensions  %}
                    {% do exceptions.raise_compiler_error("The dimension " ~ dim ~ " is not part of the metric " ~ metric_relation.name) %}
                {% else %}
                    {% do exceptions.raise_compiler_error("The dimension " ~ dim ~ " is not part of the metric " ~ metric_relation.name ~ ". If the dimension is from a custom calendar table, please create the custom_calendar_dimension_list as shown in the README.") %}
                {% endif %}
            {% endif %}

        {%endfor%}
    {%endfor%}

{% endmacro %}