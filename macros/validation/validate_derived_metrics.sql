{% macro validate_derived_metrics(metric_tree) %}

    {# We loop through the full set here to ensure that metrics that aren't listed 
    as derived are not dependent on another metric.  #}

    {% for metric_name in metric_tree.full_set %}
        {% set metric_relation = metric(metric_name)%}
        {% if metric_relation.calculation_method == "derived" and metric_relation.filters | length > 0 %}
            {%- do exceptions.raise_compiler_error("Derived metrics, such as " ~ metric_relation.name ~", do not support the use of filters. ") %}
        {%- endif %}
        {% set metric_relation_depends_on = metric_relation.metrics  | join (",") %}
        {% if metric_relation.calculation_method != "derived" and metric_relation.metrics | length > 0 %}
            {%- do exceptions.raise_compiler_error("The metric " ~ metric_relation.name ~" also references '" ~ metric_relation_depends_on ~ "' but its calculation method is '" ~ metric_relation.calculation_method ~ "'. Only metrics of calculation method derived can reference other metrics.") %}
        {%- endif %}
    {% endfor %}

{% endmacro %}