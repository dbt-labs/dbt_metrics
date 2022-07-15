{% macro validate_expression_metrics(full_set) %}

    {# We loop through the full set here to ensure that metrics that aren't listed 
    as expression are not dependent on another metric.  #}

    {% for metric_name in full_set %}
        {% set metric_relation = metric(metric_name)%}
        {% if metric_relation.type != "expression" and metric_relation.metrics | length > 0 %}
            {%- do exceptions.raise_compiler_error("The metric " ~ metric_relation.name ~ " was not an expression and dependent on another metric. This is not currently supported - if this metric depends on another metric, please change the type to expression.") %}
        {%- endif %}
    {% endfor %}

{% endmacro %}