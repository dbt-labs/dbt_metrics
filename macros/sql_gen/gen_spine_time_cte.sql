{% macro gen_spine_time_cte(metric,grain,dimensions,secondary_calculations,relevant_periods) %}
    {{ return(adapter.dispatch('gen_spine_time_cte', 'metrics')(metric,grain,dimensions,secondary_calculations,relevant_periods)) }}
{% endmacro %}

{% macro default__gen_spine_time_cte(metric,grain,dimensions,secondary_calculations,relevant_periods) %}

,{{metric.name}}__spine_time as (

    select
        calendar.date_{{grain}},

        {% if secondary_calculations | length > 0 %}
            {% for period in relevant_periods %}
                {% if period != grain%}
                    calendar.date_{{ period }},
                {% endif %}
            {% endfor %}
        {% endif %}

        {% for dim in dimensions %}
            {{metric.name}}__dims.{{ dim }}
            {% if not loop.last %},{% endif %}
        {%- endfor %}
    from calendar
    cross join {{metric.name}}__dims
)

{% endmacro %}
