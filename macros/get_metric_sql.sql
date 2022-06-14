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

{# This is awful. We have the metric.model information which returns the model name
in the ref function but in order to get JUST the model name, we have to parse through
the list of references. Additionally, the refs is a list of lists so we ALSO have to
parse through that list as well. #}
{# Awful as this is, it MIGHT be neccesary for when we have multiple metrics being 
built off of one another. We'll need to loop through the list of metrics and get
the relation for each one. #}
{# Adding expression metrics makes this more complicated because they don't have anything
within the ref function - there are no model references. #}

{# TODO Change so that the filter condition is whether metrics are present in manifest #}
{%- if metric.type == "expression" %}

    {# First we get the list of nodes that this metric is dependent on. This is inclusive 
    of all parent metrics and SHOULD only contain parent metrics #}
    {%- set node_list = metric.depends_on.nodes -%}
    {%- set metric_list = [] -%}
    {# We then loop through that list and extract the metric name. 
    TODO can we do this without extracting metric name and just matching on 
    metric_id?? #}

    {# This part is suboptimal - we're looping through the dependent nodes and extracting
    the model name from the idenitfier. Ideally we'd just use the metrics list but 
    right now its a list of lists #}
    {%- for node in node_list -%}  
        {% set metric_name = node.split('.')[2] %}
        {% do metric_list.append(metric_name) %}
    {%- endfor -%}

{% else %}

    {%- set metric_list = [] -%}
    {%- set base_model = metric.model.split('\'')[1]  -%}
    {# I NEED TO FIGURE OUT SOME WAY TO GET THE ORIGINAL MODEL RELATION #}
    {%- set model = metrics.get_model_relation(base_model if execute else "") %}
    {% do metric_list.append(metric.name) %}

{%- endif %}

{# ABOVE LOOP THOUGHTS
In multiple metric situations, what do I need?
- metric relation object, base model

What do I not need:
- grain, dimensions, secondary_calculations, start_date, end_date, where, calendar_tbl #}

{%- set calendar_tbl = ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")) %}


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
        {%- set metric = metrics.get_metric_relation(metric_object) -%}
        {%- set base_model = metric.model.split('\'')[1]  -%}
        {%- set model = metrics.get_model_relation(base_model if execute else "") %}
        {{metrics.build_metric_sql(metric,model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl)}}
    {%- endfor -%}
    
    {# If it is NOT a composite metric, we run the baseline model #}
    {%- else -%}
        {{metrics.build_metric_sql(metric,model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl)}}
{%- endif -%}

{# {{metrics.gen_final_cte(metric,metric_list,dimensions)}} #}

{% endmacro %}