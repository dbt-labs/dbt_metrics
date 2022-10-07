{%- macro gen_final_cte(metric_tree, grain, dimensions, calendar_dimensions, relevant_periods, secondary_calculations, where) -%}
    {{ return(adapter.dispatch('gen_final_cte', 'metrics')(metric_tree, grain, dimensions, calendar_dimensions, relevant_periods, secondary_calculations, where)) }}
{%- endmacro -%}

{% macro default__gen_final_cte(metric_tree, grain, dimensions, calendar_dimensions, relevant_periods, secondary_calculations, where) %}


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
    {{ metrics.gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) }}

{% else %}

    {%- if metric_tree.full_set | length > 1 %}

    select * from joined_metrics
    {#- metric where clauses -#}
        {%- if where %}
    where {{ where }}
        {%- endif -%}
    {{ metrics.gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) }}

    {% else %}

    select * from {{metric_tree.base_set[0]}}__final 
        {%- if where %}
    where {{ where }}
        {%- endif -%}
    {{ metrics.gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) }}
    
    {%- endif %}


{%- endif %}

{%- endmacro %}

{% macro redshift__gen_final_cte(metric_tree, grain, dimensions, calendar_dimensions, relevant_periods, secondary_calculations, where) %}


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

{% else %}

    {%- if metric_tree.full_set | length > 1 %}

    select * from joined_metrics
    {#- metric where clauses -#}
        {%- if where %}
    where {{ where }}
        {%- endif -%}

    {% else %}

    select * from {{metric_tree.base_set[0]}}__final 
        {%- if where %}
    where {{ where }}
        {%- endif -%}
    
    {%- endif %}


{%- endif %}

{%- endmacro %}

{% macro postgres__gen_final_cte(metric_tree, grain, dimensions, calendar_dimensions, relevant_periods, secondary_calculations, where) %}


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

{% else %}

    {%- if metric_tree.full_set | length > 1 %}

    select * from joined_metrics
    {#- metric where clauses -#}
        {%- if where %}
    where {{ where }}
        {%- endif -%}

    {% else %}

    select * from {{metric_tree.base_set[0]}}__final 
        {%- if where %}
    where {{ where }}
        {%- endif -%}
    
    {%- endif %}


{%- endif %}

{%- endmacro %}
