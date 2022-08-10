/*
    Core metric query generation logic.
    TODO:
      - allow start/end dates on metrics. Maybe special-case "today"?
      - allow passing in a seed with targets for a metric's value
*/
{%- macro get_metric_sql_deprecated(metric_name, grain, dimensions, secondary_calculations, start_date, end_date, where) %}

{# Keeping the list formatting here for ease of use #}
{% if metric_relation is iterable and (metric_relation is not string and metric_relation is not mapping) %}
    {%- do exceptions.raise_compiler_error("The metric macro does not support multiple metrics. To use this functionality, please find the information in the ReadMe of the repo and migrate to calculate.") %}
{% endif %}

{%- set metric_relation = metrics.get_metric_relation(metric_name) -%}
{% set metric_list = [metric_relation] %}
{%- set faux_metric_tree = [metric_name] -%}


{# ############
VALIDATION ROUND ONE - THE MACRO LEVEL!
############ #}

{%- set error_message = "Warning: From v0.3.0 onwards, the metric macro has been renamed to calculate and the inputs changed. Use of the original metric macro is being phased out - please switch over to the new format." -%}
{%- do exceptions.warn(error_message) -%}

{%- if not execute %}
    {%- do return("Did not execute") %}
{%- endif %}

{%- if not metric_name %}
    {%- do exceptions.raise_compiler_error("No metric or metrics provided") %}
{%- endif %}

{%- if not grain %}
    {%- do exceptions.raise_compiler_error("No date grain provided") %}
{%- endif %}

{% if where is iterable and (where is not string and where is not mapping) %}
    {%- do exceptions.raise_compiler_error("From v0.3.0 onwards, the where clause takes a single string, not a list of filters. Please fix to reflect this change") %}
{% endif %}

{%- if not metric %}
    {%- do exceptions.raise_compiler_error("No date grain provided") %}
{%- endif %}

{% for metric in metric_list %}
    {%- if metric.type == 'expression' %}
        {%- do exceptions.raise_compiler_error("Expression metrics are not supported with the deprecated metric macro. For use of expression metrics, please update to the calculate macro.") %}
    {% endif %}
{% endfor %}

{% do metrics.validate_grain(grain, faux_metric_tree, faux_metric_tree)%}

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

{# Here we are creating a list of all valid dimensions, as well as providing compilation
errors if there are any provided dimensions that don't work. #}
{% set common_valid_dimension_list = metrics.get_common_valid_dimension_list(dimensions, [metric_name]) %}

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

{# ############
LET THE COMPOSITION BEGIN!
############ #}

{# First we add the calendar table - we only need to do this once no matter how many
metrics there are #}
{{metrics.gen_calendar_cte(calendar_tbl, start_date, end_date)}}

{%- set single_metric = metric(metric_name) -%}
{%- set single_base_model = single_metric.model.split('\'')[1]  -%}
{%- set single_model = metrics.get_model_relation(single_base_model if execute else "") %}
{{ metrics.build_metric_sql(single_metric, single_model, grain, non_calendar_dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions,dimensions_provided) }}
{{ metrics.gen_secondary_calculation_cte(faux_metric_tree, non_calendar_dimensions, grain, faux_metric_tree, secondary_calculations, calendar_dimensions) }}
{{ metrics.gen_final_cte(faux_metric_tree, grain, faux_metric_tree, secondary_calculations,where) }}
    
{% endmacro %}