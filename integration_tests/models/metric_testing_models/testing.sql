{%- set metric_relation = metric('revenue') -%}

{{ log("Metric Relation: " ~ metric_relation, info=true) }}
{{ log("Metric Relation: " ~ metric_relation.model, info=true) }}
{{ log("Metric Relation: " ~ metric_relation.depends_on.nodes, info=true) }}
{{ log("Metric Relation: " ~ metric_relation.refs[0], info=true) }}