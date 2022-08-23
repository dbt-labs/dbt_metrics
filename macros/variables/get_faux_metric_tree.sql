{% macro get_faux_metric_tree(metric_list)%}

    {# This is a faux metric tree that we use for the backwards compatible metric macro #}
    {%- set metric_tree = {'full_set':metric_list} %}
    {%- do metric_tree.update({'parent_set':metric_list}) -%}
    {%- do metric_tree.update({'expression_set':metric_list}) -%}
    {%- do metric_tree.update({'base_set':metric_list}) -%}
    {%- do metric_tree.update({'ordered_expression_set':metric_list}) -%}

    {%- do return(metric_tree) -%}

{% endmacro %}