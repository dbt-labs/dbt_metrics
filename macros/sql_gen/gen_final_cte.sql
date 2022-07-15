{% macro gen_final_cte(base_set,grain,full_set,secondary_calculations) %}
    {{ return(adapter.dispatch('gen_final_cte', 'metrics')(base_set,grain,full_set,secondary_calculations)) }}
{% endmacro %}

{% macro default__gen_final_cte(base_set,grain,full_set,secondary_calculations) %}

{%- if full_set | length > 1 %}

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

        select * from {{base_set[0]}}__final 

    {% endif %}

{% endif %}

{% endmacro %}
