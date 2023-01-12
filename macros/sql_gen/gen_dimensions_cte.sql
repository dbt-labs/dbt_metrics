{%- macro gen_dimensions_cte(model_name, dimensions) -%}
    {{ return(adapter.dispatch('gen_dimensions_cte', 'metrics')(model_name, dimensions)) }}
{%- endmacro -%}

{% macro default__gen_dimensions_cte(model_name, dimensions) %}

, {{model_name}}__dims as (

    select distinct
        {%- for dim in dimensions %}
        {{ dim }}{%- if not loop.last -%},{% endif -%}
        {%- endfor %}
    from {{model_name}}__aggregate
)

{%- endmacro -%}
