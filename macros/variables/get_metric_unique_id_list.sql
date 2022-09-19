{%- macro get_metric_unique_id_list(metric) %}

    {%- if metric.metrics | length > 0 %}

        {# First we get the list of nodes that this metric is dependent on. This is inclusive 
        of all parent metrics and SHOULD only contain parent metrics #}
        {%- set node_list = metric.depends_on.nodes -%}
        {%- set metric_list = [] -%}

        {# This part is suboptimal - we're looping through the dependent nodes and extracting
        the model name from the idenitfier. Ideally we'd just use the metrics attribute but 
        right now its a list of lists #}
        {%- for node in node_list -%}  
            {%- if node.split('.')[0] == 'metric' -%}
                {% do metric_list.append(node.split('.')[2]) %} 
            {%- endif -%}
        {%- endfor -%}

    {% else %}

        {# For non-derived metrics, we just need the relation of the base model ie 
        the model that its built. Then we append it to the metric list name so the same
        variable used in derived metrics can be used below #}
        {%- set metric_list = [] -%}

    {%- endif %}

    {% do return(metric_list) %}

{% endmacro %}