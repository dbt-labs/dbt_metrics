{% macro gen_spine_time_cte(metric,grain,dimensions) %}
    {{ return(adapter.dispatch('gen_spine_time_cte', 'metrics')(metric,grain,dimensions)) }}
{% endmacro %}

{% macro default__gen_spine_time_cte(metric,grain,dimensions) %}

,{{metric.name}}__spine_time as (

    select
        calendar.date_{{grain}},

        {# I don't believe the following section is needed because we don't need other
        time periods #}
        {# {% for period in relevant_periods %}
            date_{{ period }},
        {% endfor %} #}

        {% for dim in dimensions %}
            {{metric.name}}__dims.{{ dim }}
            {% if not loop.last %},{% endif %}
        {%- endfor %}
    from calendar
    cross join {{metric.name}}__dims
)

{% endmacro %}
