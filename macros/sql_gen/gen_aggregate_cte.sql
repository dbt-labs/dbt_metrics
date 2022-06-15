{% macro gen_aggregate_cte(metric,model,grain,dimensions,secondary_calculations, start_date, end_date, where, calendar_tbl) %}
    {{ return(adapter.dispatch('gen_aggregate_cte', 'metrics')(metric,model,grain,dimensions,secondary_calculations, start_date, end_date, where, calendar_tbl)) }}
{% endmacro %}

{% macro default__gen_aggregate_cte(metric,model,grain,dimensions,secondary_calculations, start_date, end_date, where, calendar_tbl) %}

    {#- /* TODO: Do I need to validate that the requested grain is defined on the metric? */ #}
    {#- /* TODO: build a list of failures and return them all at once*/ #}
    {%- for calc_config in secondary_calculations if calc_config.aggregate %}
        {%- do metrics.validate_aggregate_coherence(metric.type, calc_config.aggregate) %}
    {%- endfor %}

    {#- /* TODO: build a list of failures and return them all at once*/ #}
    {%- for calc_config in secondary_calculations if calc_config.period %}
        {%- do metrics.validate_grain_order(grain, calc_config.period) %}
    {%- endfor %}

    {%- set relevant_periods = [] %}
    {%- for calc_config in secondary_calculations if calc_config.period and calc_config.period not in relevant_periods %}
        {%- set _ = relevant_periods.append(calc_config.period) %}
    {%- endfor -%}

    ,{{metric.name}}__aggregate as (
        {# This is the most important CTE. Instead of joining all relevant information
        and THEN aggregating, we are instead aggregating from the beginning and then 
        joining downstream for performance. Additionally, we're using a subquery instead 
        of a CTE, which was significantly more performant during our testing. #}
        select
            date_{{grain}},
            {# This is the consistent code you'll find that loops through the list of 
            dimensions. It is used throughout this macro, with slight differences to 
            account for comma syntax around loop last #}
            {% for dim in dimensions %}
                {{ dim }},
            {%- endfor %}
            {# This line performs the relevant aggregation by calling the 
            aggregate_primary_metric macro. Take a look at that one if you're curious #}
            {{- metrics.aggregate_primary_metric(metric.type, 'property_to_aggregate') }} as {{ metric.name }},
            {{ dbt_utils.bool_or('metric_date_day is not null') }} as has_data
        from ({{metrics.gen_base_query(metric,model,grain,dimensions,start_date, end_date, where, calendar_tbl)}})
        group by {{ range(1, (dimensions | length) + (relevant_periods | length) + 1 + 1) | join (", ") }}
    )

{% endmacro %}
