/*
    Core metric query generation logic.
    TODO:
      - allow start/end dates on metrics. Maybe special-case "today"?
      - allow passing in a seed with targets for a metric's value
*/
{%- macro get_metric_sql(metric, grain, dimensions, secondary_calculations, start_date, end_date, where) %}

{# ############
VALIDATION ROUND ONE - THE MACRO LEVEL!
############ #}

{%- if not execute %}
    {%- do return("not execute") %}
{%- endif %}

{%- if not metric %}
    {%- do exceptions.raise_compiler_error("No metric provided") %}
{%- endif %}

{%- if not grain %}
    {%- do exceptions.raise_compiler_error("No date grain provided") %}
{%- endif %}

{% if metric.type != "expression" and metric.metrics | length > 0 %}
    {%- do exceptions.raise_compiler_error("The metric was not an expression and dependent on another metric. This is not currently supported - if this metric depends on another metric, please change the type to expression.") %}
{%- endif %}

{# ############
LETS SET SOME VARIABLES!
############ #}

{%- set calendar_tbl = ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")) %}
{%- set dimension_list = metrics.get_valid_dimension_list(metric) -%}
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
{%- set metric_list = metrics.get_metric_list(metric) -%}

{# ############
VALIDATION ROUND TWO - CONFIG ELEMENTS!
############ #}

{#- /* TODO: Do I need to validate that the requested grain is defined on the metric? */ #}
{#- /* TODO: build a list of failures and return them all at once*/ #}
{%- for calc_config in secondary_calculations if calc_config.aggregate %}
    {%- do metrics.validate_aggregate_coherence(metric.type, calc_config.aggregate) %}
{%- endfor %}

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

{# Next we check if it is a composite metric or single metric by checking the length of the list#}
{# This filter forms the basis of how we construct the SQL #}
{%- if metric_list|length > 1 -%}

    {# If composite, we begin by looping through each of the metric names that make
    up the composite metric. #}
    {%for metric_object in metric_list%}
        {%- set loop_metric = metrics.get_metric_relation(metric_object) -%}
        {%- set loop_base_model = loop_metric.model.split('\'')[1]  -%}
        {%- set loop_model = metrics.get_model_relation(loop_base_model if execute else "") %}
        {{ metrics.build_metric_sql(loop_metric,loop_model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl,relevant_periods,calendar_dimensions) }}
        {% if loop.last %}
            {{ metrics.gen_joined_metrics_cte(metric, grain, dimensions,metric_list,calendar_dimensions) }}
            {{ metrics.gen_secondary_calculation_cte(metric,dimensions,grain,metric_list,secondary_calculations,calendar_dimensions) }}
            {{ metrics.gen_final_cte(metric,grain,secondary_calculations) }}
        {% endif %}
    {%- endfor -%}
    
    {# If it is NOT a composite metric, we run the baseline model #}
    {%- else -%}

        {# We only set these variables here because they're only needed if it isn't a 
        composite metric #}
        {%- set base_model = metric.model.split('\'')[1]  -%}
        {%- set model = metrics.get_model_relation(base_model if execute else "") %}

        {{ metrics.build_metric_sql(metric,model,grain,dimensions,secondary_calculations,start_date,end_date,where,calendar_tbl,relevant_periods,calendar_dimensions) }}
        {{ metrics.gen_secondary_calculation_cte(metric,dimensions,grain,metric_list,secondary_calculations,calendar_dimensions) }}
        {{ metrics.gen_final_cte(metric,grain,secondary_calculations,metric_list) }}
       
{%- endif -%}

{% endmacro %}