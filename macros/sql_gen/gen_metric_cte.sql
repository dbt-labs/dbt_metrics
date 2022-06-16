{% macro gen_metric_cte(metric, grain, dimensions) %}
    {{ return(adapter.dispatch('gen_metric_cte', 'metrics')(metric, grain, dimensions)) }}
{% endmacro %}

{% macro default__gen_metric_cte(metric,grain,dimensions) %}

,{{metric.name}}__final as (
    
    select
        {{metric.name}}__spine_time.date_{{grain}},
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
