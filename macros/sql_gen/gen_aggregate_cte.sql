{%- macro gen_aggregate_cte(metrics_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, total_dimension_count, group_name, group_values) -%}
    {{ return(adapter.dispatch('gen_aggregate_cte', 'metrics')(metrics_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, total_dimension_count, group_name, group_values)) }}
{%- endmacro -%}

{%- macro default__gen_aggregate_cte(metrics_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, total_dimension_count, group_name, group_values) %}

, {{group_name}}__aggregate as (
    {# This is the most important CTE. Instead of joining all relevant information
    and THEN aggregating, we are instead aggregating from the beginning and then 
    joining downstream for performance. Additionally, we're using a subquery instead 
    of a CTE, which was significantly more performant during our testing. -#}
    {#- #}
    select

        {%- if grain %}
        date_{{grain}},

        {#- All of the other relevant periods that aren't currently selected as the grain
        are neccesary for downstream secondary calculations. We filter it on whether 
        there are secondary calculations to reduce the need for overhead -#}
            {%- if secondary_calculations | length > 0 -%}
                {%- for period in relevant_periods %}
        date_{{ period }},
                {%- endfor -%}
            {% endif -%}
        {%- endif -%}

        {#- This is the consistent code you'll find that loops through the list of 
        dimensions. It is used throughout this macro, with slight differences to 
        account for comma syntax around loop last -#}
        {%- for dim in dimensions %}
        {{ dim }},
        {%- endfor %}

        {%- for calendar_dim in calendar_dimensions %}
        {{ calendar_dim }},
        {% endfor -%}

        {%- if grain %}
        {{ bool_or('metric_date_day is not null') }} as has_data,
        {%- endif %}

        {#- This line performs the relevant aggregation by calling the 
        gen_primary_metric_aggregate macro. Take a look at that one if you're curious -#}
        {%- for metric_name in group_values.metric_names -%} 
        {{ metrics.gen_primary_metric_aggregate(metrics_dictionary[metric_name].calculation_method, 'property_to_aggregate__'~metric_name) }} as {{ metric_name }}
        {%- if not loop.last -%},{%- endif -%}
        {%- endfor%}
    from ({{ metrics.gen_base_query(
                metrics_dictionary=metrics_dictionary,
                grain=grain, 
                dimensions=dimensions, 
                secondary_calculations=secondary_calculations, 
                start_date=start_date, 
                end_date=end_date, 
                relevant_periods=relevant_periods, 
                calendar_dimensions=calendar_dimensions,
                total_dimension_count=total_dimension_count,
                group_name=group_name,
                group_values=group_values
                )
            }}
    ) as base_query

    where 1=1
    {#- 
        Given that we've already determined the metrics in metric_names share
        the same windows & filters, we can base the conditional off of the first 
        value in the list because the order doesn't matter. 
     -#}
    {%- if group_values.window is not none and grain %}
    and date_{{grain}} = window_filter_date
    {%- endif %}
    {{ metrics.gen_group_by(grain, dimensions, calendar_dimensions, relevant_periods) }}

)

{%- endmacro -%}
