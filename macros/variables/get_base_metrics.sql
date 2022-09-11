{% macro get_base_metrics(metric) %}

    -- this checks whether it is a relation or a list
    {%- if (metric is mapping and metric.get('metadata', {}).get('calculation_method', '').endswith('Relation')) %}

        {%- for child in metric recursive -%}

            {%- if metric.metrics | length > 0 %}

            {# First we get the list of nodes that this metric is dependent on. This is inclusive 
            of all parent metrics and SHOULD only contain parent metrics #}
            {%- set node_list = metric.depends_on.nodes -%}
            {%- set metric_list = [] -%}
            {# This part is suboptimal - we're looping through the dependent nodes and extracting
            the metric name from the idenitfier. Ideally we'd just use the metrics attribute but 
            right now its a list of lists #}
                {%- for node in node_list -%}  
                    {% set metric_name = node.split('.')[2] %}
                    {% do metric_list.append(metric_name) %}
                {%- endfor -%}
            {%- endif -%}
        {%- endfor -%}

    {% else %}

        {# For non-derived metrics, we just need the relation of the base model ie 
        the model that its built. Then we append it to the metric list name so the same
        variable used in derived metrics can be used below #}
        {%- set metric_list = [] -%}
        {% do metric_list.append(metric.name) %}

    {%- endif %}

    {% do return(metric_list) %}

{% endmacro %}