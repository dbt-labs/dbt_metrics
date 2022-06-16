{% macro gen_final_cte(metric,grain,secondary_calculations) %}
    {{ return(adapter.dispatch('gen_final_cte', 'metrics')(metric,grain,secondary_calculations)) }}
{% endmacro %}

{% macro default__gen_final_cte(metric,grain,secondary_calculations) %}

{%- if metric.metrics | length > 0 %}

    {% if secondary_calculations | length > 0 %}

        ,final as (

            select
                *
                {% for calc_config in secondary_calculations %}
                    ,{{ metrics.generate_secondary_calculation_alias(calc_config, grain) }}
                {% endfor %}
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
                {% for calc_config in secondary_calculations %}
                    ,{{ metrics.generate_secondary_calculation_alias(calc_config, grain) }}
                {% endfor %}
            from secondary_calculations
        )

        select * from final 

        {% else %}

        -- single metric without secondary calculations

        select * from {{metric.name}}__final 

    {% endif %}

{% endif %}

{% endmacro %}
