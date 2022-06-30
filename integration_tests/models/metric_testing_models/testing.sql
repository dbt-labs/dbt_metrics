{%- set metric_relation = metric('total_profit') -%}
{%- set calendar_tbl = ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")) %}

{%- set metric_tree = {'full_set':[],'leaf_set':[],'expression_set':[],'base_set':[]} -%}
{%- do metric_tree.update({'base_set':metric_relation.name}) -%}
{%- set metrics_list = graph.metrics.values() -%}

{% set full_set = metrics.get_metric_tree(metric_relation ,metric_tree) %}
{{ log("Metric Tree: " ~ full_set, info=true) }}

select 1 as column_name