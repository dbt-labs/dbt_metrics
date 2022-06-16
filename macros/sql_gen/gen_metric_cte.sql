{% macro gen_metric_cte(metric, grain, dimensions,secondary_calculations,relevant_periods) %}
    {{ return(adapter.dispatch('gen_metric_cte', 'metrics')(metric, grain, dimensions,secondary_calculations,relevant_periods)) }}
{% endmacro %}

{% macro default__gen_metric_cte(metric,grain,dimensions,secondary_calculations,relevant_periods) %}

,{{metric.name}}__final as (
    
    select
        {{metric.name}}__spine_time.date_{{grain}},
        
        {% if secondary_calculations | length > 0 %}
            {% for period in relevant_periods %}
                {{metric.name}}__spine_time.date_{{ period }},
            {% endfor %}
        {% endif %}
        
        {% for dim in dimensions %}
            {{metric.name}}__spine_time.{{ dim }},
        {%- endfor %}
        ifnull({{metric.name}}, 0) as {{metric.name}}
        
    from {{metric.name}}__spine_time
    left outer join {{metric.name}}__aggregate
        using (date_{{grain}},
                {% for dim in dimensions %}
                    {{ dim }}
                    {% if not loop.last %},{% endif %}
                {%- endfor %})

)

{% endmacro %}
