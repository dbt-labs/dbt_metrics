{% macro gen_final_cte(metric,grain,dimensions,secondary_calculations) %}
    {{ return(adapter.dispatch('gen_final_cte', 'metrics')(metric,grain,dimensions,secondary_calculations)) }}
{% endmacro %}

{% macro default__gen_final_cte(metric,grain,dimensions,secondary_calculations) %}

{%- if metric.metrics | length > 0 %}

    {% if secondary_calculations | length > 0 %}

        ,final as (

            select
                *
            from secondary_calculations
        )

        select * from final 

    {% else %}

    select * from joined_metrics

    {% endif %}

{% else %}

    {% if secondary_calculations | length > 0 %}

        -- single metric with secondary calculations
        
        ,final as (

            select
                *
            from secondary_calculations
        )

        select * from final 

        {% else %}

        -- single metric without secondary calculations

        select * from {{metric.name}}__final 

    {% endif %}

{% endif %}

{% endmacro %}
