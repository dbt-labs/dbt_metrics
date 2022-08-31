{%- macro gen_dimensions_cte(metric_name, dimensions) -%}
    {{ return(adapter.dispatch('gen_dimensions_cte', 'metrics')(metric_name, dimensions)) }}
{%- endmacro -%}

{% macro default__gen_dimensions_cte(metric_name, dimensions) %}

, {{metric_name}}__dims as (
    select distinct
        {% for dim in dimensions %}
        {{ dim }}{%- if not loop.last -%},{% endif -%}
        {%- endfor %}
        
    from {{metric_name}}__aggregate
)

{%- endmacro -%}
