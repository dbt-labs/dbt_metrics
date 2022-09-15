{%- macro gen_spine_time_cte(metric_name, grain, dimensions, secondary_calculations, relevant_periods, calendar_dimensions, dimensions_provided) -%}
    {{ return(adapter.dispatch('gen_spine_time_cte', 'metrics')(metric_name, grain, dimensions, secondary_calculations, relevant_periods, calendar_dimensions, dimensions_provided)) }}
{%- endmacro -%}

{% macro default__gen_spine_time_cte(metric_name, grain, dimensions, secondary_calculations, relevant_periods, calendar_dimensions, dimensions_provided) %}

, {{metric_name}}__spine_time as (

    select
        calendar.date_{{grain}}

        {%- if secondary_calculations | length > 0 -%}
            {% for period in relevant_periods %}
                {%- if period != grain -%}
        , calendar.date_{{ period }}
                {%- endif -%}
            {% endfor -%}
        {% endif -%}

        {% for calendar_dim in calendar_dimensions %}
        , calendar.{{ calendar_dim }}
        {%- endfor %}

        {%- for dim in dimensions %}
        , {{metric_name}}__dims.{{ dim }}
        {%- endfor %}

    from calendar
    {%- if dimensions_provided %}
    cross join {{metric_name}}__dims
    {%- endif %}
    {{ metrics.gen_group_by(grain,dimensions,calendar_dimensions,relevant_periods) }}

)
{%- endmacro -%}
