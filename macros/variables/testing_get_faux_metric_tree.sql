{% macro testing_get_faux_metric_tree(metric_list,develop_yml)%}

    {%- set metric_tree = {'full_set':[]} %}
    {%- do metric_tree.update({'parent_set':[]}) -%}
    {%- do metric_tree.update({'expression_set':[]}) -%}
    {%- do metric_tree.update({'base_set':metric_list}) -%}
    {%- do metric_tree.update({'ordered_expression_set':{}}) -%}

    
    {# {%- set full_set = [] %} #}

    {% for metric_definition in develop_yml.metrics %}
        {%- set metric_tree = metrics.update_faux_metric_tree(metric_definition, metric_tree, develop_yml) -%}
    {% endfor %}

    {# {%- do metric_tree.update({'full_set':full_set}) -%} #}
    {%- do metric_tree.update({'full_set':set(full_set)}) -%}
    {%- do metric_tree.update({'parent_set':set(parent_set)}) -%}
    {%- do metric_tree.update({'expression_set':set(expression_set)}) -%}

    {% do print(metric_tree) %}

    {%- do return(metric_tree) -%}

{% endmacro %}