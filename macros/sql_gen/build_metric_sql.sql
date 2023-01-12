{%- macro build_metric_sql(metrics_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, dimensions_provided, total_dimension_count, model_name, model_values) %}
    
    {#- This is the SQL Gen part - we've broken each component out into individual macros -#}
    {#- We broke this out so it can loop for composite metrics -#}
    {{ metrics.gen_aggregate_cte(
        metrics_dictionary=metrics_dictionary,
        grain=grain, 
        dimensions=dimensions, 
        secondary_calculations=secondary_calculations,
        start_date=start_date, 
        end_date=end_date, 
        relevant_periods=relevant_periods, 
        calendar_dimensions=calendar_dimensions,
        total_dimension_count=total_dimension_count,
        model_name=model_name,
        model_values=model_values
    ) }}
    
    {#- Diverging path for secondary calcs and needing to datespine -#}
    {%- if grain and secondary_calculations | length > 0 -%}

        {%- if dimensions_provided == true -%}
        
            {{ metrics.gen_dimensions_cte(
                model_name=model_name, 
                dimensions=dimensions
            ) }}
        
        {%- endif -%}

        {{ metrics.gen_spine_time_cte(
            model_name=model_name, 
            grain=grain, 
            dimensions=dimensions, 
            secondary_calculations=secondary_calculations, 
            relevant_periods=relevant_periods, 
            calendar_dimensions=calendar_dimensions, 
            dimensions_provided=dimensions_provided
        )}}

    {%- endif -%}

    {{ metrics.gen_metric_cte(
        metrics_dictionary=metrics_dictionary,
        model_name=model_name, 
        model_values=model_values,
        grain=grain, 
        dimensions=dimensions, 
        secondary_calculations=secondary_calculations, 
        start_date=start_date, 
        end_date=end_date, 
        relevant_periods=relevant_periods, 
        calendar_dimensions=calendar_dimensions
    )}} 

{%- endmacro -%}
