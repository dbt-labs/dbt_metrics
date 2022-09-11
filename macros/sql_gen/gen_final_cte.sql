{%- macro gen_final_cte(metric_tree, grain, secondary_calculations, where) -%}
    {{ return(adapter.dispatch('gen_final_cte', 'dbt_metrics')(metric_tree, grain, secondary_calculations, where)) }}
{%- endmacro -%}

{% macro default__gen_final_cte(metric_tree, grain, secondary_calculations, where) %}

{%- if metric_tree.full_set | length > 1 %}

    {%- if secondary_calculations | length > 0 -%}

, final as (

    select
        *
    from secondary_calculations
)

select * from final 

{# metric where clauses #}
{%- if where %}
where {{ where }}
{%- endif %}

{%- else -%}

select * from joined_metrics

{#- metric where clauses -#}
{%- if where %}
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

{#- metric where clauses -#}
{%- if where %}
where {{ where }}
{%- endif -%}

    {%- else -%}

-- single metric without secondary calculations

select * from {{metric_tree.base_set[0]}}__final 


{#- metric where clauses -#}
{%- if where %}
where {{ where }}
{%- endif -%}

{%- endif -%}

{%- endif -%}

{%- endmacro %}
