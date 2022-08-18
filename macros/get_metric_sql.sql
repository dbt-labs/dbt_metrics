/*
    Core metric query generation logic.
    TODO:
      - allow start/end dates on metrics. Maybe special-case "today"?
      - allow passing in a seed with targets for a metric's value
*/
{%- macro get_metric_sql(metric_list, grain, dimensions, secondary_calculations, start_date, end_date, where, initiated_by,metric_definition=None) %}

{# ############
VALIDATION AROUND METRIC VS CALCULATE VS DEVELOP
############ #}

{% if initiated_by == 'calculate' %}
    {% set is_calculate_macro = true %}
    {% set is_develop_macro = false %}
{% elif initiated_by == 'develop' %}
    {% set is_calculate_macro = false %}
    {% set is_develop_macro = true %}
{% endif %}

{# ############
METRIC ATTRIBUTES TO VARIABLES
We do this so that we can support metric attributes coming in from other formats
like develop. This is easier than altering all of the downstream logic to have flags
for calculate vs develop
############ #}
{% if is_develop_macro %}
    {% set metric_name = metric_definition["name"]%}
    {% set metric_type = metric_definition["type"]%}
    {% set metric_sql = metric_definition["sql"]%}
    {% set metric_timestamp = metric_definition["timestamp"]%}
    {% set metric_time_grains = metric_definition["time_grains"]%}
    {% set metric_dimensions = metric_definition["dimensions"]%}
    {% set metric_filters = metric_definition["filters"]%}
    {% set metric_base_model = metric_definition["model"].replace('"','\'').split('\'')[1]  %}
    {% set metric_model = metrics.get_model_relation(metric_base_model if execute else "") %}
{% endif %}
 
{# ############
VARIABLE SETTING ROUND 1: List Vs Single Metric!
############ #}

{% if is_calculate_macro and metric_list is not iterable %}
    {% set metric_list = [metric_list] %}
{% elif is_develop_macro %}
    {% set metric_list = [metric_definition["name"]] %}
{% endif %}

{# We are creating the metric tree here - this includes all the leafs (first level parents)
, the expression metrics, and the full combination of them both #}
{% if is_calculate_macro %}
    {% set metric_tree = metrics.get_metric_tree(metric_list) %}
{% elif is_develop_macro %}
    {% set metric_tree = metrics.get_faux_metric_tree(metric_list) %}
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

{% if where is iterable and (where is not string and where is not mapping) %}
    {%- do exceptions.raise_compiler_error("From v0.3.0 onwards, the where clause takes a single string, not a list of filters. Please fix to reflect this change") %}
{% endif %}

{% if not is_develop_macro %}
    {% do metrics.validate_grain(grain, metric_tree['full_set'], metric_tree['base_set'])%}
{% endif %}

{% if not is_develop_macro %}
    {% do metrics.validate_expression_metrics(metric_tree['full_set'])%}
{% endif %}

{# ############
LETS SET SOME VARIABLES AND VALIDATE!
############ #}

{# Setting a variable to denote if the user has provided any dimensions #}
{% if dimensions | length > 0 %}
    {% set dimensions_provided = true %}
{% else %}
    {% set dimensions_provided = false %}
{% endif %}

{# Here we set the calendar table as a variable, which ensures the default overwritten if they include
a custom calendar #}
{%- set calendar_tbl = ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")) %}

{% if is_develop_macro %}
    {%- set calendar_dimensions = metrics.get_calendar_dimension_list(metric_dimensions, dimensions) -%}
    {%- set non_calendar_dimensions = metrics.get_non_calendar_dimension_list(dimensions) -%}
    {%- set relevant_periods = metrics.get_relevent_periods(grain, secondary_calculations) %}

{% else %}
    {# Here we are creating a list of all valid dimensions, as well as providing compilation
    errors if there are any provided dimensions that don't work. #}
    {% set common_valid_dimension_list = metrics.get_common_valid_dimension_list(dimensions, metric_tree['full_set']) %}

    {# We have to break out calendar dimensions as their own list of acceptable dimensions. 
    This is because of the date-spining. If we don't do this, it creates impossible combinations
    of calendar dimension + base dimensions #}
    {%- set calendar_dimensions = metrics.get_calendar_dimension_list(dimensions, common_valid_dimension_list) -%}

    {# Additionally, we also have to restrict the dimensions coming in from the macro to 
    no longer include those we've designated as calendar dimensions. That way they 
    are correctly handled by the spining. We override the dimensions variable for 
    cleanliness #}
    {%- set non_calendar_dimensions = metrics.get_non_calendar_dimension_list(dimensions) -%}

    {# Finally we set the relevant periods, which is a list of all time grains that need to be contained
    within the final dataset in order to accomplish base + secondary calc functionality. #}
    {%- set relevant_periods = metrics.get_relevent_periods(grain, secondary_calculations) %}

{% endif %}

{# ############
VALIDATION ROUND TWO - CONFIG ELEMENTS!
############ #}

{#- /* TODO: #49 Do I need to validate that the requested grain is defined on the metric? */ #}
{#- /* TODO: build a list of failures and return them all at once*/ #}
{% for metric in metric_list %}
    {% if not is_develop_macro %}
        {% set metric_type = metric.type%}
    {% endif %}
    {%- for calc_config in secondary_calculations if calc_config.aggregate %}
        {%- do metrics.validate_aggregate_coherence(metric_type, calc_config.aggregate) %}
    {%- endfor %}
{%endfor%}

{#- /* TODO: build a list of failures and return them all at once*/ #}
{%- for calc_config in secondary_calculations if calc_config.period %}
    {%- do metrics.validate_grain_order(grain, calc_config.period) %}
{%- endfor %}

{# ############
LET THE COMPOSITION BEGIN!
############ #}

{# First we add the calendar table - we only need to do this once no matter how many
metrics there are #}
{{metrics.gen_calendar_cte(calendar_tbl, start_date, end_date)}}

{# TODO - Have everything in one loop #}

{# Next we check if it is a composite metric or single metric by checking the length of the list#}
{# This filter forms the basis of how we construct the SQL #}
{%- if metric_tree["full_set"]|length > 1 -%}

    {# If composite, we begin by looping through each of the metric names that make
    up the composite metric. #}

    {% for metric_name in metric_tree["parent_set"]%}
        {% set loop_metric = metrics.get_metric_relation(metric_name) %}

        {# Here we set the metric parameters. Previously we just provided the metric object
        but in order to support develop logic we need to break these down into their own 
        variables #}
        {% set metric_name = loop_metric.name%}
        {% set metric_type = loop_metric.type%}
        {% set metric_sql = loop_metric.sql%}
        {% set metric_timestamp = loop_metric.timestamp%}
        {% set metric_time_grains = loop_metric.time_grains%}
        {% set metric_dimensions = loop_metric.dimensions%}
        {% set metric_filters = loop_metric.filters%}
        {% set metric_base_model = loop_metric.model.replace('"','\'').split('\'')[1]  %}
        {% set metric_model = metrics.get_model_relation(metric_base_model if execute else "") %}

        {{ metrics.build_metric_sql(metric_name, metric_type, metric_sql, metric_timestamp, metric_filters, metric_model, grain, non_calendar_dimensions, secondary_calculations, start_date, end_date,calendar_tbl, relevant_periods, calendar_dimensions,dimensions_provided) }}
    {% endfor %}

    {{ metrics.gen_joined_metrics_cte(metric_tree["parent_set"], metric_tree["expression_set"], metric_tree["ordered_expression_set"], grain, non_calendar_dimensions, calendar_dimensions, secondary_calculations, relevant_periods) }}
    {{ metrics.gen_secondary_calculation_cte(metric_tree["base_set"], non_calendar_dimensions, grain, metric_tree["full_set"], secondary_calculations, calendar_dimensions) }}
    {{ metrics.gen_final_cte(metric_tree["base_set"], grain, metric_tree["full_set"], secondary_calculations,where) }}
    

{# If we're calling the develop macro then we don't need to loop through the metrics because we know 
this is only a single metric and not an expression metric #}
{%- elif is_develop_macro -%}

    {{ metrics.build_metric_sql(metric_name, metric_type, metric_sql, metric_timestamp, metric_filters, metric_model, grain, non_calendar_dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions, dimensions_provided) }}
    {{ metrics.gen_secondary_calculation_cte(metric_tree["base_set"], non_calendar_dimensions, grain, metric_tree["full_set"], secondary_calculations, calendar_dimensions) }}
    {{ metrics.gen_final_cte(metric_tree["base_set"], grain, metric_tree["full_set"], secondary_calculations,where) }}

{# If it is NOT a composite metric, we run the baseline model #}
{%- else -%}

    {# We only set these variables here because they're only needed if it isn't a 
    composite metric #}

    {% for metric_name in metric_tree["full_set"]%}
        {%- set single_metric = metric(metric_name) -%}

        {# Here we set the metric parameters. Previously we just provided the metric object
        but in order to support develop logic we need to break these down into their own 
        variables #}
        {% set metric_name = single_metric.name%}
        {% set metric_type = single_metric.type%}
        {% set metric_sql = single_metric.sql%}
        {% set metric_timestamp = single_metric.timestamp%}
        {% set metric_time_grains = single_metric.time_grains%}
        {% set metric_dimensions = single_metric.dimensions%}
        {% set metric_filters = single_metric.filters%}
        {% set metric_base_model = single_metric.model.replace('"','\'').split('\'')[1]  %}
        {% set metric_model = metrics.get_model_relation(metric_base_model if execute else "") %}

        {{ metrics.build_metric_sql(metric_name, metric_type, metric_sql, metric_timestamp, metric_filters, metric_model, grain, non_calendar_dimensions, secondary_calculations, start_date, end_date,calendar_tbl, relevant_periods, calendar_dimensions,dimensions_provided) }}
    {% endfor %}
    {{ metrics.gen_secondary_calculation_cte(metric_tree["base_set"], non_calendar_dimensions, grain, metric_tree["full_set"], secondary_calculations, calendar_dimensions) }}
    {{ metrics.gen_final_cte(metric_tree["base_set"], grain, metric_tree["full_set"], secondary_calculations,where) }}
    
{%- endif -%}

{% endmacro %}