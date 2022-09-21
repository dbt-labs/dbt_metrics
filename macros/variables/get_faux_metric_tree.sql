{% macro get_faux_metric_tree(metric_list,develop_yml)%}

    {%- set metric_tree = {'full_set':[]} %}
    {%- do metric_tree.update({'parent_set':[]}) -%}
    {%- do metric_tree.update({'expression_set':[]}) -%}
    {%- do metric_tree.update({'base_set':metric_list}) -%}
    {%- do metric_tree.update({'ordered_expression_set':{}}) -%}

    
    {%- set full_set = [] %}

    {% for metric_definition in develop_yml.metrics %}

        {% if metric_definition.calculation_method != 'derived' %}
            {% set parent_set = metric_tree['parent_set'] %}
            {% do parent_set.append(metric_definition.name)%}
            {% do full_set.append(metric_definition.name)%}
            {%- do metric_tree.update({'parent_set':parent_set}) -%}
        {% endif %}

        {% if metric_definition.calculation_method == 'derived' %}
            {% set expression_set = metric_tree['expression_set'] %}
            {% do expression_set.append(metric_definition.name)%}
            {% do full_set.append(metric_definition.name)%}
            {%- do metric_tree.update({'expression_set':expression_set}) -%}
        {% endif %}
        {%- do metric_tree.update({'ordered_expression_set':metric_definition.name}) -%}
    {% endfor %}
    {%- do metric_tree.update({'full_set':full_set}) -%}


    {% do print(metric_tree) %}

    {%- do return(metric_tree) -%}

{% endmacro %}