/*
    Core metric query generation logic.
    TODO:
      - validate that the requested dim is actually an option (or fail at query execution instead of at compilation if they don't exist? is it a problem to expose columns that exist in the table but aren't "blessed" for the metric?)
      - allow start/end dates on metrics. Maybe special-case "today"?
      - allow passing in a seed with targets for a metric's value
*/


{%- macro get_metric_sql(metric, grain, dimensions, secondary_calculations, start_date, end_date, where) %}
{%- if not execute %}
    {%- do return("not execute") %}
{%- endif %}

{%- if not metric %}
    {%- do exceptions.raise_compiler_error("No metric provided") %}
{%- endif %}

{%- if not grain %}
    {%- do exceptions.raise_compiler_error("No date grain provided") %}
{%- endif %}


{# TODO Change so that the filter condition is whether metrics are present in manifest #}
{%- if metric.type == "expression" %}

    {# First we get the list of nodes that this metric is dependent on. This is inclusive 
    of all parent metrics and SHOULD only contain parent metrics #}
    {%- set node_list = metric.depends_on.nodes -%}
    {%- set metric_list = [] -%}

    {# This part is suboptimal - we're looping through the dependent nodes and extracting
    the model name from the idenitfier. Ideally we'd just use the metrics attribute but 
    right now its a list of lists #}
    {%- for node in node_list -%}  
        {% set metric_name = node.split('.')[2] %}
        {% do metric_list.append(metric_name) %}
    {%- endfor -%}

{% else %}

    {# For non-expression metrics, we just need the relation of the base model ie 
    the model that its built. Then we append it to the metric list name so the same
    variable used in expression metrics can be used below #}
    {%- set metric_list = [] -%}
    {%- set base_model = metric.model.split('\'')[1]  -%}
    {%- set model = metrics.get_model_relation(base_model if execute else "") %}
    {% do metric_list.append(metric.name) %}

{%- endif %}

{# Here we set the calendar as either being the default provided by the package
or the variable provided in the project #}
{%- set calendar_tbl = ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")) %}
{% set calendar_dims = dbt_utils.get_filtered_columns_in_relation(from=ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar"))) %}

{% set calendar_dimensions = [] %}
{% for dim in calendar_dims %}
    {% do calendar_dimensions.append(dim | lower) %}
{% endfor %}

{# Here we are going to ensure that the metrics provided are accurate and that they are present 
in either the metric definition or the default/custom calendar table #}
{%- set dimension_list = [] -%}
{%- for dim in dimensions -%}
    {%- do dimension_list.append(metrics.is_valid_dimension(metric,dim,calendar_dimensions)) -%}
{%- endfor -%}
{%- set dimensions=dimension_list -%}

{# ############
LET THE COMPOSITION BEGIN!
############ #}

{# First we add the calendar table - we only need to do this once no matter how many
metrics there are #}
{{metrics.gen_calendar_cte(calendar_tbl,start_date,end_date)}}

{# Next we check if it is a composite metric or single metric by checking the length of the list#}
{%- if metric_list|length > 1 -%}
    
    {# If composite, we begin by creating a blank list of the cte names to use
    later and then iterate through each metric to form the query that builds the
    information we need. #}
    {%for metric_object in metric_list%}
        {%- set loop_metric = metrics.get_metric_relation(metric_object) -%}
        {%- set base_model = loop_metric.model.split('\'')[1]  -%}
        {%- set model = metrics.get_model_relation(base_model if execute else "") %}
        {{metrics.build_metric_sql(loop_metric,model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl)}}
    {%- endfor -%}
    
    {# If it is NOT a composite metric, we run the baseline model #}
    {%- else -%}
        {{metrics.build_metric_sql(metric,model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl)}}

{%- endif -%}

{{ metrics.gen_final_cte(metric, grain, metric_list, dimensions) }}

{% endmacro %}