{%- macro gen_aggregate_cte(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions) -%}
    {{ return(adapter.dispatch('gen_aggregate_cte', 'metrics')(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions)) }}
{%- endmacro -%}

{%- macro default__gen_aggregate_cte(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions) %}

, {{metric_dictionary.name}}__aggregate as (
    {# This is the most important CTE. Instead of joining all relevant information
    and THEN aggregating, we are instead aggregating from the beginning and then 
    joining downstream for performance. Additionally, we're using a subquery instead 
    of a CTE, which was significantly more performant during our testing. -#}
    select

        {%- if grain != 'all_time' %}
        date_{{grain}},

        {#- All of the other relevant periods that aren't currently selected as the grain
        are neccesary for downstream secondary calculations. We filter it on whether 
        there are secondary calculations to reduce the need for overhead -#}
            {%- if secondary_calculations | length > 0 -%}
                {%- for period in relevant_periods %}
        date_{{ period }},
                {% endfor -%}
            {% endif -%}
        {% endif -%}

        {#- This is the consistent code you'll find that loops through the list of 
        dimensions. It is used throughout this macro, with slight differences to 
        account for comma syntax around loop last -#}
        {% for dim in dimensions %}
        {{ dim }},
        {%- endfor %}

        {%- for calendar_dim in calendar_dimensions %}
        {{ calendar_dim }},
        {% endfor -%}

        {#- This line performs the relevant aggregation by calling the 
        gen_primary_metric_aggregate macro. Take a look at that one if you're curious -#}
        {{ metrics.gen_primary_metric_aggregate(metric_dictionary.calculation_method, 'property_to_aggregate') }} as {{ metric_dictionary.name }},

        {%- if grain != 'all_time' %}
        {{ dbt_utils.bool_or('metric_date_day is not null') }} as has_data
        {% else %}
        min(metric_date_day) as metric_start_date,
        max(metric_date_day) as metric_end_date
        {% endif %}

    from ({{ metrics.gen_base_query(
                metric_dictionary=metric_dictionary,
                grain=grain, 
                dimensions=dimensions, 
                secondary_calculations=secondary_calculations, 
                start_date=start_date, 
                end_date=end_date, 
                calendar_tbl=calendar_tbl, 
                relevant_periods=relevant_periods, 
                calendar_dimensions=calendar_dimensions) }}
    ) as base_query

    where 1=1

    {% if metric_dictionary.window is not none %}
    and date_{{grain}} = window_filter_date
    {% endif %}

    {{ metrics.gen_group_by(grain, dimensions, calendar_dimensions, relevant_periods) }}

)

{%- endmacro -%}
