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
    {% set single_metric = metric_list %}
    {% set is_multi_metric = false %}

{% elif metric_list | length == 1 %}
    {% set single_metric = metric_list[0] %}
    {% set is_multi_metric = false %}

{% else %}

    {% set is_multi_metric = true %}

{% endif %}


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

{# #### TODO: CREATE MACRO FOR ERROR MESSAGE
        - reshape so we can loop in all circumstances 
  #}

{% if not is_multi_metric %}
    {% for metric in metric_list %}
        {% if metric.type != "expression" and metric.metrics | length > 0 %}
            {%- do exceptions.raise_compiler_error("The metric " ~ metric.name ~ " was not an expression and dependent on another metric. This is not currently supported - if this metric depends on another metric, please change the type to expression.") %}
        {%- endif %}
    {% endfor %}
{% else %}
    {% if single_metric.type != "expression" and single_metric.metrics | length > 0 %}
        {%- do exceptions.raise_compiler_error("The metric " ~ single_metric.name ~ " was not an expression and dependent on another metric. This is not currently supported - if this metric depends on another metric, please change the type to expression.") %}
    {%- endif %}
{%- endif %}

{# ############
LETS SET SOME VARIABLES!
############ #}

{# We are creating the metric tree here - this includes all the leafs (first level parents)
, the expression metrics, and the full combination of them both #}
{%- set metric_tree = {'full_set':[],'leaf_set':[],'expression_set':[],'base_set':[],'ordered_expression_set':{}} -%}

{% if metric_list is iterable and (metric_list is not string and metric_list is not mapping) %} 
    {% set base_set_list = []%}
    {% for metric in metric_list %}
        {%- do base_set_list.append(metric.name) -%}
        {%- set metric_tree = metrics.get_metric_tree(metric ,metric_tree) -%}
    {%endfor%}
    {%- do metric_tree.update({'base_set':base_set_list}) -%}

{% else %}
    {%- do metric_tree.update({'base_set':single_metric.name}) -%}
    {%- set metric_tree = metrics.get_metric_tree(single_metric ,metric_tree) -%}
{% endif %}

{# Now we will iterate over the metric tree and make it a unique list to account for duplicates #}
{% set full_set = [] %}
{% set leaf_set = [] %}
{% set expression_set = [] %}
{% set base_set = [] %}

{% for metric in metric_tree['full_set']|unique%}
    {% do full_set.append(metric)%}
{% endfor %}
{%- do metric_tree.update({'full_set':full_set}) -%}

{% for metric in metric_tree['leaf_set']|unique%}
    {% do leaf_set.append(metric)%}
{% endfor %}
{%- do metric_tree.update({'leaf_set':leaf_set}) -%}


{% for metric in metric_tree['expression_set']|unique%}
    {% do expression_set.append(metric)%}
{% endfor %}
{%- do metric_tree.update({'expression_set':expression_set}) -%}

{% for metric in metric_tree['leaf_set']|unique%}
    {%- do metric_tree['ordered_expression_set'].pop(metric) -%}
{% endfor %}

{# This section overrides the expression set by ordering the metrics on their depth so they 
can be correctly referenced in the downstream sql query #}
{% set ordered_expression_list = []%}
{% for item in metric_tree['ordered_expression_set']|dictsort(false, 'value') %}
    {% if item[0] in metric_tree["expression_set"]%}
        {% do ordered_expression_list.append(item[0])%}
    {% endif %}
{% endfor %}
{%- do metric_tree.update({'expression_set':ordered_expression_list}) -%}

{# Here we set the calendar table as a variable, which ensures the default overwritten if they include
a custom calendar #}
{%- set calendar_tbl = ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")) %}

{# Here we are creating the dimension list which has the list of all dimensions that 
are a part of the metric. This has additional logic for multiple metrics #}
{% if metric_list is iterable and (metric_list is not string and metric_list is not mapping) %} 
    {%- set dimension_list = [] -%}
    {% for metric in metric_list %}
        {%- set metric_dimensions= metrics.get_valid_dimension_list(metric) -%}
        {%- set new_dimensions = ( metric_dimensions | reject('in',dimension_list) | list) -%}
        {% for dim in new_dimensions %}
            {%- do dimension_list.append(dim) -%}
        {% endfor %}
    {%endfor%}

{% else %}
    {%- set dimension_list = metrics.get_valid_dimension_list(single_metric) -%}
{% endif %}

{# We have to break out calendar dimensions as their own list of acceptable dimensions. 
This is because of the date-spining. If we don't do this, it creates impossible combinations
of calendar dimension + base dimensions #}
{%- set calendar_dimensions = metrics.get_calendar_dimension_list(dimensions,dimension_list) -%}

{# Additionally, we also have to restrict the dimensions coming in from the macro to 
no longer include those we've designated as calendar dimensions. That way they 
are correctly handled by the spining. We override the dimensions variable for 
cleanliness#}
{%- set dimensions = metrics.get_non_calendar_dimension_list(dimensions) -%}
{%- set relevant_periods = metrics.get_relevent_periods(grain, secondary_calculations) %}

{# ############
VALIDATION ROUND TWO - CONFIG ELEMENTS!
############ #}

{#- /* TODO: Do I need to validate that the requested grain is defined on the metric? */ #}
{#- /* TODO: build a list of failures and return them all at once*/ #}
{% if metric_list is iterable and (metric_list is not string and metric_list is not mapping) %} 
    {% for metric in metric_list %}
        {%- for calc_config in secondary_calculations if calc_config.aggregate %}
            {%- do metrics.validate_aggregate_coherence(metric.type, calc_config.aggregate) %}
        {%- endfor %}
    {%endfor%}

{% else %}
    {%- for calc_config in secondary_calculations if calc_config.aggregate %}
        {%- do metrics.validate_aggregate_coherence(single_metric.type, calc_config.aggregate) %}
    {%- endfor %}
{% endif %}


{#- /* TODO: build a list of failures and return them all at once*/ #}
{%- for calc_config in secondary_calculations if calc_config.period %}
    {%- do metrics.validate_grain_order(grain, calc_config.period) %}
{%- endfor %}

{# This section will validate that the metric dimensions are contained within the appropriate 
list of metrics #}

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

    {% for metric_name in metric_tree["leaf_set"]%}
        {%- set loop_metric = metrics.get_metric_relation(metric_name) -%}
        {%- set loop_base_model = loop_metric.model.split('\'')[1]  -%}
        {%- set loop_model = metrics.get_model_relation(loop_base_model if execute else "") %}
        {{ metrics.build_metric_sql(loop_metric,loop_model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl,relevant_periods,calendar_dimensions) }}
    {% endfor %}

    {{ metrics.gen_joined_metrics_cte(metric_tree["leaf_set"],metric_tree["expression_set"],metric_tree["ordered_expression_set"], grain, dimensions,calendar_dimensions,secondary_calculations,relevant_periods) }}
    {{ metrics.gen_secondary_calculation_cte(metric_tree["base_set"],dimensions,grain,metric_tree["full_set"],secondary_calculations,calendar_dimensions) }}
    {{ metrics.gen_final_cte(metric_tree["base_set"],grain,metric_tree["full_set"],secondary_calculations) }}
    
    {# If it is NOT a composite metric, we run the baseline model #}
{%- else -%}

    {# We only set these variables here because they're only needed if it isn't a 
    composite metric #}
    {%- set base_model = single_metric.model.split('\'')[1]  -%}
    {%- set model = metrics.get_model_relation(base_model if execute else "") %}

    {{ metrics.build_metric_sql(single_metric,model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl,relevant_periods,calendar_dimensions) }}
    {{ metrics.gen_secondary_calculation_cte(metric_tree["base_set"],dimensions,grain,metric_tree["full_set"],secondary_calculations,calendar_dimensions) }}
    {{ metrics.gen_final_cte(metric_tree["base_set"],grain,metric_tree["full_set"],secondary_calculations) }}
    
{%- endif -%}

{% endmacro %}