{%- macro build_metric_sql(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions, dimensions_provided) %}
    
    {%- set treat_null_values_as_zero = metric_dictionary.get("config").get("treat_null_values_as_zero", True)  -%}
    {#- This is the SQL Gen part - we've broken each component out into individual macros -#}
    {#- We broke this out so it can loop for composite metrics -#}
    {{ metrics.gen_aggregate_cte(
        metric_dictionary=metric_dictionary,
        grain=grain, 
        dimensions=dimensions, 
        secondary_calculations=secondary_calculations,
        start_date=start_date, 
        end_date=end_date, 
        calendar_tbl=calendar_tbl, 
        relevant_periods=relevant_periods, 
        calendar_dimensions=calendar_dimensions
    ) }}
    
    {#- Adding conditional logic to exclude the unique combinations of dimensions if there are no dimensions -#}
    {%- if grain != "all_time" -%}

        {%- if dimensions_provided == true -%}
        
            {{ metrics.gen_dimensions_cte(
                metric_name=metric_dictionary.name, 
                dimensions=dimensions
            ) }}
        
        {%- endif -%}


        {{ metrics.gen_spine_time_cte(
            metric_name=metric_dictionary.name, 
            grain=grain, 
            dimensions=dimensions, 
            secondary_calculations=secondary_calculations, 
            relevant_periods=relevant_periods, 
            calendar_dimensions=calendar_dimensions, 
            dimensions_provided=dimensions_provided
        )}}

    {%- endif -%}

    {{ metrics.gen_metric_cte(
        metric_name=metric_dictionary.name, 
        grain=grain, 
        dimensions=dimensions, 
        secondary_calculations=secondary_calculations, 
        start_date=start_date, 
        end_date=end_date, 
        relevant_periods=relevant_periods, 
        calendar_dimensions=calendar_dimensions,
        treat_null_values_as_zero=treat_null_values_as_zero
    )}}

{% endmacro -%}
