{% macro get_faux_metric_tree(metric_list,develop_yml)%}

    {%- set metric_tree = {'full_set':[]} %}
    {%- do metric_tree.update({'parent_set':[]}) -%}
    {%- do metric_tree.update({'expression_set':[]}) -%}
    {%- do metric_tree.update({'base_set':metric_list}) -%}
    {%- do metric_tree.update({'ordered_expression_set':{}}) -%}

    {% for metric_name in metric_list %}
        {% set metric_definition = develop_yml[metric_name]%}
        {%- set metric_tree = metrics.update_faux_metric_tree(metric_definition, metric_tree, develop_yml) -%}
    {% endfor %}

    {%- do metric_tree.update({'full_set':set(metric_tree['full_set'])}) -%}
    {%- do metric_tree.update({'parent_set':set(metric_tree['parent_set'])}) -%}
    {%- do metric_tree.update({'expression_set':set(metric_tree['expression_set'])}) -%}

    {% for metric_name in metric_tree['parent_set']|unique%}
        {%- do metric_tree['ordered_expression_set'].pop(metric_name) -%}
    {% endfor %}

    {# This section overrides the derived set by ordering the metrics on their depth so they 
    can be correctly referenced in the downstream sql query #}
    {% set ordered_expression_list = []%}
    {% for item in metric_tree['ordered_expression_set']|dictsort(false, 'value') %}
        {% if item[0] in metric_tree["expression_set"]%}
            {% do ordered_expression_list.append(item[0])%}
        {% endif %}
    {% endfor %}
    {%- do metric_tree.update({'expression_set':ordered_expression_list}) -%}

    {%- do return(metric_tree) -%}

{% endmacro %}