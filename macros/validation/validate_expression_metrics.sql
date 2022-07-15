{% macro validate_expression_metrics(metric_names) %}

    {# We loop through the full set here to ensure that metrics that aren't listed 
    as expression are not dependent on another metric.  #}

    {% for metric_name in metric_names %}
        {% set metric_relation = metric(metric_name)%}
        {% set metric_relation_depends_on = metric_relation.metrics  | join (",") %}
        {% if metric_relation.type != "expression" and metric_relation.metrics | length > 0 %}
            {%- do exceptions.raise_compiler_error("The metric " ~ metric_relation.name ~" also references '" ~ metric_relation_depends_on ~ "' but its type is '" ~ metric_relation.type ~ "'. Only metrics of type expression can reference other metrics.") %}
        {%- endif %}
    {% endfor %}

{% endmacro %}