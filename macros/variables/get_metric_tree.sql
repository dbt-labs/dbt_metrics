{% macro get_metric_tree(metric_list)%}

{# We are creating the metric tree here - this includes all the leafs (first level parents)
, the derived metrics, and the full combination of them both #}

{# This line creates the metric tree dictionary and the full_set key. 
Full Set contains ALL metrics that are referenced, which includes metrics in the macro
AND all parent/derived metrics. #}
{%- set metric_tree = {'full_set':[]} %}
{# The parent set is a list of parent metrics that are NOT derived metrics. IE if 
metric C is built off of metric A and B, A and B would be the parent metrics because they 
are both upstream of Metric C AND not derived metrics themselves. #}
{%- do metric_tree.update({'parent_set':[]}) -%}
{# The derived set is a list of derived metrics. This includes all derived metrics referenced
in the macro itself OR upstream of the metrics referenced in the macro #}
{%- do metric_tree.update({'derived_set':[]}) -%}
{# The base set is the list of metrics that are provided into the macro #}
{%- do metric_tree.update({'base_set':[]}) -%}
{# The ordered derived set is the list of derived metrics that are ordered based on their
node depth. So if Metric C were downstream of Metric A and B, which were also derived metrics,
Metric C would have the value of 999 (max depth) and A and B would have 998, representing that they
are one depth upstream #}
{%- do metric_tree.update({'ordered_derived_set':{}}) -%}

{% set base_set_list = []%}
{% for metric in metric_list %}
    {%- do base_set_list.append(metric.name) -%}
    {%- set metric_tree = metrics.update_metric_tree(metric ,metric_tree) -%}
{% endfor %}
{%- do metric_tree.update({'base_set':base_set_list}) -%}

{# Now we will iterate over the metric tree and make it a unique list to account for duplicates #}
{% set full_set = [] %}
{% set parent_set = [] %}
{% set derived_set = [] %}
{% set base_set = [] %}

{% for metric_name in metric_tree['full_set']|unique%}
    {% do full_set.append(metric_name)%}
{% endfor %}
{%- do metric_tree.update({'full_set':full_set}) -%}

{% for metric_name in metric_tree['parent_set']|unique%}
    {% do parent_set.append(metric_name)%}
{% endfor %}
{%- do metric_tree.update({'parent_set':parent_set}) -%}

{% for metric_name in metric_tree['derived_set']|unique%}
    {% do derived_set.append(metric_name)%}
{% endfor %}
{%- do metric_tree.update({'derived_set':derived_set}) -%}

{% for metric in metric_tree['parent_set']|unique%}
    {%- do metric_tree['ordered_derived_set'].pop(metric) -%}
{% endfor %}

{# This section overrides the derived set by ordering the metrics on their depth so they 
can be correctly referenced in the downstream sql query #}
{% set ordered_expression_list = []%}
{% for item in metric_tree['ordered_derived_set']|dictsort(false, 'value') %}
    {% if item[0] in metric_tree["derived_set"]%}
        {% do ordered_expression_list.append(item[0])%}
    {% endif %}
{% endfor %}
{%- do metric_tree.update({'derived_set':ordered_expression_list}) -%}

{%- do return(metric_tree) -%}

{% endmacro %}