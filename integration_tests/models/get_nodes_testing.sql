    
{%- set metric_relation = metric('ratio_metric') -%}
{% set parent_list = metric_relation.derived_metric_dependency%}
{% do print(parent_list)%}


