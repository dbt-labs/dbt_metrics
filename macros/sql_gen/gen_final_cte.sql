{% macro gen_final_cte(base_set,grain,full_set,secondary_calculations, where) %}
    {{ return(adapter.dispatch('gen_final_cte', 'metrics')(base_set,grain,full_set,secondary_calculations, where)) }}
{% endmacro %}

{% macro default__gen_final_cte(base_set,grain,full_set,secondary_calculations, where) %}

{%- if full_set | length > 1 %}

    {%- if secondary_calculations | length > 0 -%}

        ,final as (

            select
                *
            from secondary_calculations
        )

        select * from final 

            -- metric where clauses...
        {% if where %}
        where {{ where }}
        {% endif %}

    {% else %}

    select * from joined_metrics

    -- metric where clauses...
    {%- if where -%}
        where {{ where }}
    {%- endif -%}

    {%- endif %}

{% else %}

    {%- if secondary_calculations | length > 0 %}

        -- single metric with secondary calculations
        
        , final as (

            select
                *
            from secondary_calculations
        )

        select * from final 

        -- metric where clauses...
        {%- if where %}
        where {{ where }}
        {% endif -%}

        {%- else -%}

        -- single metric without secondary calculations

        select * from {{base_set[0]}}__final 


        -- metric where clauses...
        {%- if where -%}
        where {{ where }}
        {%- endif -%}

    {% endif %}

{% endif %}

{% endmacro %}
