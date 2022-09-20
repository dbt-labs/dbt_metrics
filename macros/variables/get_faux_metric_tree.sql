{% macro get_faux_metric_tree(metric_list,develop_yml)%}

    {%- set metric_tree = {'full_set':[]} %}
    {%- do metric_tree.update({'parent_set':[]}) -%}
    {%- do metric_tree.update({'expression_set':[]}) -%}
    {%- do metric_tree.update({'base_set':[]}) -%}
    {%- do metric_tree.update({'ordered_expression_set':{}}) -%}

    {% for metric_definition in develop_yml.metrics %}

        {%- do metric_tree.update({'base_set':metric_definition.name}) %}

        {% if metric_definition.calculation_method != 'derived' %}
            {%- do metric_tree.update({'parent_set':metric_definition.name}) -%}
        {% endif %}
        {% if metric_definition.calculation_method == 'derived' %}
            {%- do metric_tree.update({'expression_set':metric_definition.name}) -%}
        {% endif %}
        {%- do metric_tree.update({'ordered_expression_set':metric_definition.name}) -%}
        {%- do metric_tree.update({'full_set':metric_definition.name}) -%}
    {% endfor %}

    {% do print(metric_tree) %}

    {%- do return(metric_tree) -%}

{% endmacro %}