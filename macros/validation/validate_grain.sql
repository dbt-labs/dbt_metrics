{% macro validate_grain(grain, metric_tree, metrics_dictionary, secondary_calculations, dimensions) %}

    {# We loop through the full set here to ensure that the provided grain works for all metrics
    returned or used, not just those listed #}
    {% if grain %}
        {%- if not grain and secondary_calculations | length > 0 -%}
            {%- do exceptions.raise_compiler_error("Secondary calculations require a grain to be provided") -%}
        {%- endif -%}


        {% for metric_name in metric_tree.full_set %}
            {% set metric_relation = metrics_dictionary[metric_name]%}

            {% if grain not in metric_relation.time_grains%}
                {% if metric_name not in metric_tree.base_set %}
                    {%- do exceptions.raise_compiler_error("The metric " ~ metric_name ~ " is an upstream metric of one of the provided metrics. The grain " ~ grain ~ " is not defined in its metric definition.") %}
                {% else %}
                    {%- do exceptions.raise_compiler_error("The metric " ~ metric_name ~ " does not have the provided time grain " ~ grain ~ " defined in the metric definition.") %}
                {% endif %}
            {% endif %}
        {% endfor %}

    {% elif not grain %}
        {% for metric_name in metric_tree.full_set %}
            {% set metric_relation = metrics_dictionary[metric_name]%}
            {% if metric_relation.get("config").get("restrict_no_time_grain", False) == True %}
                {% if metric_name not in metric_tree.base_set %}
                    {%- do exceptions.raise_compiler_error("The metric " ~ metric_relation.name ~ " is an upstream metric of one of the provided metrics and has been configured to not allow non time-grain queries.") %}
                {% else %}
                    {%- do exceptions.raise_compiler_error("The metric " ~ metric_relation.name ~ " has been configured to not allow non time-grain queries.") %}
                {% endif %}
            {% endif %}

        {% endfor %}

        {% if secondary_calculations | length > 0 %}
            {%- do exceptions.raise_compiler_error("Using secondary calculations without a grain is not supported.") %}
        {% endif %}

        {% for metric_name in metric_tree.full_set %}
            {% if metrics_dictionary[metric_name].window is not none%}
                {%- do exceptions.raise_compiler_error("Aggregating without a grain does not support metrics with window definitions.") %}
            {% endif%}
        {% endfor%}

    {% endif %}

{% endmacro %}