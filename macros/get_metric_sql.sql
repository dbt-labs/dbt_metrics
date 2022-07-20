/*
    Core metric query generation logic.
    TODO:
      - allow start/end dates on metrics. Maybe special-case "today"?
      - allow passing in a seed with targets for a metric's value
*/
{%- macro get_metric_sql(metric_list, grain, dimensions, secondary_calculations, start_date, end_date, where) %}

{# ############
VARIABLE SETTING ROUND 1: List Vs Single Metric!
############ #}

{% if metric_list is not iterable %}
    {% set metric_list = [metric_list] %}
{% endif %}

{# We are creating the metric tree here - this includes all the leafs (first level parents)
, the expression metrics, and the full combination of them both #}
{%- set metric_tree = metrics.get_metric_tree(metric_list) %}

{# ############
VALIDATION ROUND ONE - THE MACRO LEVEL!
############ #}

{%- if not execute %}
    {%- do return("Did not execute") %}
{%- endif %}

{%- if not metric_list %}
    {%- do exceptions.raise_compiler_error("No metric or metrics provided") %}
{%- endif %}

{%- if not grain %}
    {%- do exceptions.raise_compiler_error("No date grain provided") %}
{%- endif %}

{% do metrics.validate_grain(grain, metric_tree['full_set'])%}

{% do metrics.validate_expression_metrics(metric_tree['full_set'])%}

{# ############
LETS SET SOME VARIABLES!
############ #}

{# Here we set the calendar table as a variable, which ensures the default overwritten if they include
a custom calendar #}
{%- set calendar_tbl = ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")) %}

{# Here we are creating the dimension list which has the list of all dimensions that 
are a part of the metric. This has additional logic for multiple metrics #}
    {# TODO #48 get valid common dimension name to clarify what is #}
{%- set dimension_list = [] -%}
{% for metric in metric_list %}
    {# get all dimensions in metric  #}
    {%- set metric_dimensions= metrics.get_valid_dimension_list(metric) -%}
    {# This line removes any of the dimensions for loop metric that don't exist in the dimension list already
    This creates a smaller list that only contains the subset #}

    {# TODO #50 build common list across all metrics and then find ones that appear X (metric count) times  #}
    {%- set new_dimensions = ( metric_dimensions | reject('in',dimension_list) | list) -%}
    {% for dim in new_dimensions %}
        {%- do dimension_list.append(dim) -%}
    {% endfor %}
{%endfor%}

{# We have to break out calendar dimensions as their own list of acceptable dimensions. 
This is because of the date-spining. If we don't do this, it creates impossible combinations
of calendar dimension + base dimensions #}
{%- set calendar_dimensions = metrics.get_calendar_dimension_list(dimensions,dimension_list) -%}

{# Additionally, we also have to restrict the dimensions coming in from the macro to 
no longer include those we've designated as calendar dimensions. That way they 
are correctly handled by the spining. We override the dimensions variable for 
cleanliness #}
{%- set dimensions = metrics.get_non_calendar_dimension_list(dimensions) -%}
{%- set relevant_periods = metrics.get_relevent_periods(grain, secondary_calculations) %}

{# ############
VALIDATION ROUND TWO - CONFIG ELEMENTS!
############ #}

{#- /* TODO: #49 Do I need to validate that the requested grain is defined on the metric? */ #}
{#- /* TODO: build a list of failures and return them all at once*/ #}
{% for metric in metric_list %}
    {%- for calc_config in secondary_calculations if calc_config.aggregate %}
        {%- do metrics.validate_aggregate_coherence(metric.type, calc_config.aggregate) %}
    {%- endfor %}
{%endfor%}

{#- /* TODO: build a list of failures and return them all at once*/ #}
{%- for calc_config in secondary_calculations if calc_config.period %}
    {%- do metrics.validate_grain_order(grain, calc_config.period) %}
{%- endfor %}

{# This section will validate that the metric dimensions are contained within the appropriate 
list of metrics #}

{# rename dimensions to denote that this is a filtered list of the base input from the macro #}

{%- for dim in dimensions -%}
    {% do metrics.is_valid_dimension(dim, dimension_list)  %}
{%- endfor %}

{# ############
LET THE COMPOSITION BEGIN!
############ #}

{# First we add the calendar table - we only need to do this once no matter how many
metrics there are #}
{{metrics.gen_calendar_cte(calendar_tbl,start_date,end_date)}}

{# TODO - Have everything in one loop #}

{# Next we check if it is a composite metric or single metric by checking the length of the list#}
{# This filter forms the basis of how we construct the SQL #}
{%- if metric_tree["full_set"]|length > 1 -%}

    {# If composite, we begin by looping through each of the metric names that make
    up the composite metric. #}

    {% for metric_name in metric_tree["parent_set"]%}
        {%- set loop_metric = metrics.get_metric_relation(metric_name) -%}
        {%- set loop_base_model = loop_metric.model.split('\'')[1]  -%}
        {%- set loop_model = metrics.get_model_relation(loop_base_model if execute else "") %}
        {{ metrics.build_metric_sql(loop_metric,loop_model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl,relevant_periods,calendar_dimensions) }}
    {% endfor %}

    {{ metrics.gen_joined_metrics_cte(metric_tree["parent_set"],metric_tree["expression_set"],metric_tree["ordered_expression_set"], grain, dimensions,calendar_dimensions,secondary_calculations,relevant_periods) }}
    {{ metrics.gen_secondary_calculation_cte(metric_tree["base_set"],dimensions,grain,metric_tree["full_set"],secondary_calculations,calendar_dimensions) }}
    {{ metrics.gen_final_cte(metric_tree["base_set"],grain,metric_tree["full_set"],secondary_calculations) }}
    
    {# If it is NOT a composite metric, we run the baseline model #}
{%- else -%}

    {# We only set these variables here because they're only needed if it isn't a 
    composite metric #}

    {% for metric_name in metric_tree["full_set"]%}
        {%- set single_metric = metric(metric_name) -%}
        {%- set single_base_model = single_metric.model.split('\'')[1]  -%}
        {%- set single_model = metrics.get_model_relation(single_base_model if execute else "") %}
        {{ metrics.build_metric_sql(single_metric,single_model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl,relevant_periods,calendar_dimensions) }}
    {% endfor %}
    {{ metrics.gen_secondary_calculation_cte(metric_tree["base_set"],dimensions,grain,metric_tree["full_set"],secondary_calculations,calendar_dimensions) }}
    {{ metrics.gen_final_cte(metric_tree["base_set"],grain,metric_tree["full_set"],secondary_calculations) }}
    
{%- endif -%}

{% endmacro %}