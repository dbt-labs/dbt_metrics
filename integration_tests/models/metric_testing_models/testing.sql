{%- set metric_relation = metric('total_profit') -%}
{%- set calendar_tbl = ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")) %}

{%- set metric_tree = {} -%}
{%- set metrics_list = graph.metrics.values() -%}

{{ log("Node Unique ID: " ~ metric_relation.unique_id, info=true) }}
{{ log("Depends on: " ~ metric_relation.depends_on, info=true) }}
{{ log("Depends on nodes: " ~ metric_relation.depends_on.nodes, info=true) }}
{{ log("Depends on metrics: " ~ metric_relation.metrics, info=true) }}
{% set test_list = metrics.get_metric_unique_id_list(metric_relation) %}
{{ log("Test List: " ~ test_list, info=true) }}
{% set full_set = metrics.get_metric_parents(metric_relation ,metric_tree) %}
{{ log("Full Set: " ~ full_set, info=true) }}

select 1 as column_name