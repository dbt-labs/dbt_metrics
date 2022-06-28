{% macro get_nodes_testing()%}
    
    {%- set metric_relation = metric('total_profit') -%}
    {{ log("MACRO: Node Unique ID: " ~ metric_relation.unique_id, info=true) }}
    {{ log("MACRO: Depends on: " ~ metric_relation.depends_on, info=true) }}
    {{ log("MACRO: Depends on nodes: " ~ metric_relation.depends_on.nodes, info=true) }}

{% endmacro %}