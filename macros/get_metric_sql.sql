{%- macro get_metric_sql(metrics_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, where, metric_tree) %}

{#- ############
TODO: All validation occurs in calculate and develop - please reference there for any validation
############ -#}

{#- ############
LETS SET SOME VARIABLES!
############ -#}

{#- We have to break out calendar dimensions as their own list of acceptable dimensions. 
This is because of the date-spining. If we don't do this, it creates impossible combinations
of calendar dimension + base dimensions -#}
{%- set calendar_dimensions = metrics.get_calendar_dimensions(dimensions) -%}

{#- Additionally, we also have to restrict the dimensions coming in from the macro to 
no longer include those we've designated as calendar dimensions. That way they 
are correctly handled by the spining. We override the dimensions variable for 
cleanliness -#}
{%- set non_calendar_dimensions = metrics.get_non_calendar_dimension_list(dimensions, var('custom_calendar_dimension_list',[])) -%}

{#- Finally we set the relevant periods, which is a list of all time grains that need to be contained
within the final dataset in order to accomplish base + secondary calc functionality. -#}
{%- set relevant_periods = metrics.get_relevent_periods(grain, secondary_calculations) -%}

{# Setting a variable to denote if the user has provided any dimensions #}
{%- if non_calendar_dimensions | length > 0 -%}
    {%- set dimensions_provided = true -%}
{%- else -%}
    {%- set dimensions_provided = false -%}
{%- endif -%}

{#- Here we set the calendar table as a variable, which ensures the default overwritten if they include
a custom calendar -#}
{%- set calendar_tbl = ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")) -%}

{# Here we get the total dimension count for grouping #}
{%- set total_dimension_count = metrics.get_total_dimension_count(grain, dimensions, calendar_dimensions, relevant_periods) -%}

{#- ############
LET THE COMPOSITION BEGIN!
############ -#}

{#- First we add the calendar table - we only need to do this once no matter how many
metrics there are -#}
{{ metrics.gen_calendar_cte(
    calendar_tbl=calendar_tbl,
    start_date=start_date, 
    end_date=end_date) 
    }}

{#- Next we check if it is a composite metric or single metric by checking the length of the list -#}
{#- This filter forms the basis of how we construct the SQL -#}

{#- If composite, we begin by looping through each of the metric names that make
up the composite metric. -#}

{%- for metric_name in metric_tree["parent_set"] -%}

    --TODO: Make this branching
    {{ metrics.build_metric_sql(
        metric_dictionary=metrics_dictionary[metric_name], 
        grain=grain, 
        dimensions=non_calendar_dimensions, 
        secondary_calculations=secondary_calculations, 
        start_date=start_date, 
        end_date=end_date,
        calendar_tbl=calendar_tbl, 
        relevant_periods=relevant_periods,
        calendar_dimensions=calendar_dimensions,
        dimensions_provided=dimensions_provided,
        total_dimension_count=total_dimension_count
    ) 
    }}

{%- endfor -%}

{%- if metric_tree["full_set"] | length > 1 -%}

    -- TODO: Make this permissive with non date-spined metrics
    {{ metrics.gen_joined_metrics_cte(
        metric_tree=metric_tree,
        grain=grain, 
        dimensions=non_calendar_dimensions, 
        calendar_dimensions=calendar_dimensions, 
        secondary_calculations=secondary_calculations, 
        relevant_periods=relevant_periods,
        metrics_dictionary=metrics_dictionary,
        total_dimension_count=total_dimension_count ) 
    }}

{% endif -%}

{%- if secondary_calculations | length > 0 -%}

    --TODO Make this optional for secondary calcs
    {{ metrics.gen_secondary_calculation_cte(
        metric_tree=metric_tree,
        grain=grain, 
        dimensions=non_calendar_dimensions, 
        secondary_calculations=secondary_calculations, 
        calendar_dimensions=calendar_dimensions,
        metric_dictionary=metrics_dictionary 
        ) 
        }}

{%- endif -%}

--TODO: Make sure this pulls from the right CTEs
{{ metrics.gen_final_cte(
    metric_tree=metric_tree,
    grain=grain, 
    dimensions=non_calendar_dimensions, 
    calendar_dimensions=calendar_dimensions, 
    relevant_periods=relevant_periods,
    secondary_calculations=secondary_calculations,
    where=where) 
    }}

{% endmacro %}