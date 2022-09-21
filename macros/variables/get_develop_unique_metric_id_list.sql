{%- macro get_develop_unique_metric_id_list(metric_definition) %}

    {% set re = modules.re %}

    {%- set metric_list = [] -%}

    {%- if metric_definition.calculation_method == 'derived' %}

        {# First we get the list of nodes that this metric is dependent on. This is inclusive 
        of all parent metrics and SHOULD only contain parent metrics #}
        {%- set dependency_metrics = re.findall("'[^']+'",metric_definition.expression) -%}

        {# This part is suboptimal - we're looping through the dependent nodes and extracting
        the model name from the idenitfier. Ideally we'd just use the metrics attribute but 
        right now its a list of lists #}
        {%- for metric_name in dependency_metrics -%} 
=           {% do metric_list.append(metric_name.replace('\'','')) %} 
        {%- endfor -%}

    {%- endif %}

    {% do return(metric_list) %}

{% endmacro %}