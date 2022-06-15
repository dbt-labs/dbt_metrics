{% macro gen_joined_cte(metric, grain, dimensions) %}
    {{ return(adapter.dispatch('gen_joined_cte', 'metrics')(metric, grain, dimensions)) }}
{% endmacro %}

{% macro default__gen_joined_cte(metric,grain,dimensions) %}

,{{metric.name}}__final as (
    
    select
        date_{{grain}},
        {% for dim in dimensions %}
            {{ dim }},
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
