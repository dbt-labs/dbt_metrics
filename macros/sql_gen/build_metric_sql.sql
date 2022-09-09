{%- macro build_metric_sql(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions, dimensions_provided) %}
    
    {#- This is the SQL Gen part - we've broken each component out into individual macros -#}
    {#- We broke this out so it can loop for composite metrics -#}
    {{ dbt_metrics.gen_aggregate_cte(
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
        
            {{ dbt_metrics.gen_dimensions_cte(
                metric_name=metric_dictionary.name, 
                dimensions=dimensions
            ) }}
        
        {%- endif -%}


        {{ dbt_metrics.gen_spine_time_cte(
            metric_name=metric_dictionary.name, 
            grain=grain, 
            dimensions=dimensions, 
            secondary_calculations=secondary_calculations, 
            relevant_periods=relevant_periods, 
            calendar_dimensions=calendar_dimensions, 
            dimensions_provided=dimensions_provided
        )}}

    {%- endif -%}

    {{ dbt_metrics.gen_metric_cte(
        metric_name=metric_dictionary.name, 
        grain=grain, 
        dimensions=dimensions, 
        secondary_calculations=secondary_calculations, 
        start_date=start_date, 
        end_date=end_date, 
        relevant_periods=relevant_periods, 
        calendar_dimensions=calendar_dimensions
    )}}

{% endmacro -%}
