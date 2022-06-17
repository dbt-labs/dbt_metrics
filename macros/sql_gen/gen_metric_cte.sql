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

    {% if not start_date or not end_date%}
        where (
        {% if not start_date and not end_date %}
            {{metric.name}}__spine_time.date_{{grain}} >= (select min(case when has_data then date_{{grain}} end) from {{metric.name}}__aggregate)
            and {{metric.name}}__spine_time.date_{{grain}} <= (select max(case when has_data then date_{{grain}} end) from {{metric.name}}__aggregate)
        {% elif not start_date and end_date %}
            {{metric.name}}__spine_time.date_{{grain}} >= (select min(case when has_data then date_{{grain}} end) from {{metric.name}}__aggregate)
        {% elif start_date and not end_date %}
            {{metric.name}}__spine_time.date_{{grain}} <= (select max(case when has_data then date_{{grain}} end) from {{metric.name}}__aggregate)
        {% endif %} 
        )      
    {% endif %} 

)

{% endmacro %}
