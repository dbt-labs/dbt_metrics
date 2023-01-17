{%- macro gen_dimensions_cte(group_name, dimensions) -%}
    {{ return(adapter.dispatch('gen_dimensions_cte', 'metrics')(group_name, dimensions)) }}
{%- endmacro -%}

{% macro default__gen_dimensions_cte(group_name, dimensions) %}

, {{group_name}}__dims as (

    select distinct
        {%- for dim in dimensions %}
        {{ dim }}{%- if not loop.last -%},{% endif -%}
        {%- endfor %}
    from {{group_name}}__aggregate
)

{%- endmacro -%}
