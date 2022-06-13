{%- set metric_relation = metric('total_profit') -%}

{{ log("Metric Base Value: " ~ metric_relation, info=true) }}
{{ log("Metric Model: " ~ metric_relation.model, info=true) }}
{{ log("Metric Depends On.Nodes: " ~ metric_relation.depends_on.nodes, info=true) }}
{{ log("Metric Refs[0]: " ~ metric_relation.refs[0], info=true) }}
{{ log("Metric Dimensions: " ~ metric_relation.dimensions, info=true) }}
