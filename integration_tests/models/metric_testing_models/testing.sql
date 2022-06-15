{%- set metric_relation = metric('total_profit') -%}
{%- set calendar_tbl = ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")) %}


{# {%- set node_list = metric_relation.depends_on.nodes -%}
{%- set metric_list = [] -%}
{%- set cte_list = [] -%}
{%- for node in node_list -%}  
    {% set metric_name = node.split('.')[2] %}
    {% do metric_list.append(metric_name) %}
{%- endfor -%} #}

{# {{ log("Calendar Table: " ~ calendar_tbl, info=true) }}
{%for metric_object in metric_list%}
    {{ log("Metric Object Name: " ~ metric_object, info=true) }}
    {%- set loop_metric = metrics.get_metric_relation(metric_object) -%}
    {{ log("Metric Object: " ~ loop_metric, info=true) }}
    {%- set base_model = loop_metric.model.split('\'')[1]  -%}
    {{ log("Metric Base Model: " ~ base_model, info=true) }}
    {%- set model = metrics.get_model_relation(base_model if execute else "") %}
    {{ log("Metric Model: " ~ model, info=true) }}
    {% do cte_list.append(loop_metric.name) %}
    {% if loop.last %}
        {{ log("CTE List: " ~ cte_list, info=true) }}
        {{ log("Metric List: " ~ metric_list, info=true) }}
        {{ log("Metric: " ~ metric, info=true) }}
    {% endif %}

{%- endfor -%} #}

{% set calendar_dims = dbt_utils.get_filtered_columns_in_relation(from=ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")))%}
{{ log("Calendar Dims: " ~ calendar_dims, info=true) }}
{%- set dimension_list = metric_relation.dimensions | list -%}
{{ log("Metric Dimensions: " ~ dimension_list, info=true) }}
{% for dim in calendar_dims %}
    {% do dimension_list.append(dim | lower) %}
{%endfor%}
{{ log("Metric Dimensions: " ~ dimension_list, info=true) }}

{# {% set total_dims = dimension_list | list  + calendar_dims  %}
{{ log("Total Dimensions: " ~ total_dims, info=true) }} #}

{# {%- set dimension_list = [] -%}
{%- set dimensions = ['had_discount','order_country'] -%} #}
{# {%- for dim in dimensions -%}
    {% do dimension_list.append(metrics.is_valid_dimension(metric_relation,dim,calendar_dims)) %}
{%endfor%}  #}
{# {{ log("Dimension List: " ~ dimensions, info=true) }} #}

{# {{ log("Metric Base Value: " ~ metric_relation, info=true) }}
{{ log("Metric Model: " ~ metric_relation.model, info=true) }}
{{ log("Metric Type: " ~ metric_relation.type, info=true) }}
{{ log("Metric Name: " ~ metric_relation.name, info=true) }}
{{ log("Metric Depends On.Nodes: " ~ metric_relation.depends_on.nodes, info=true) }}
{{ log("Metric Refs[0]: " ~ metric_relation.refs[0], info=true) }}
{{ log("Metric Dimensions: " ~ metric_relation.dimensions, info=true) }}
{{ log("Metric Metric Call: " ~ metric_list[0], info=true) }}
{{ log("Metric Node List: " ~ testing_list, info=true) }}
{{ log("Metric Split: " ~ metric_split, info=true) }}
{{ log("Metric Metric Object: " ~ metric, info=true) }} #}

{# This is here so the model can run #}
select 1 as column_name