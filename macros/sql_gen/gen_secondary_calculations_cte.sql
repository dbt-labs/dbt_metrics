{%- macro gen_secondary_calculation_cte(metric_tree, grain, dimensions, secondary_calculations, calendar_dimensions) -%}
    {{ return(adapter.dispatch('gen_secondary_calculation_cte', 'dbt_metrics')(metric_tree, grain, dimensions, secondary_calculations, calendar_dimensions)) }}
{%- endmacro -%}

{% macro default__gen_secondary_calculation_cte(metric_tree, grain, dimensions, secondary_calculations, calendar_dimensions) %}

{#- The logic for secondary calculations is past the point where having calendar + dim
in a single list would create issues. So here we join them together. Plus it makes it
easier for not having to update the working secondary calc logic -#}
{%- set dimensions = dimensions+calendar_dimensions -%}

, secondary_calculations as (

    select 
        *
        
        {#- Checking if base_set is a list - which you'd think would have its own test but no
        Jinja doesn't have that built in so we have to hack it by checking if it is an 
        iterable variable and NOT sring /mapping -#}
        {%- if metric_tree.base_set is iterable and (metric_tree.base_set is not string and metric_tree.base_set is not mapping) -%} 

            {%- for metric_name in metric_tree.base_set -%}

                {%- for calc_config in secondary_calculations %}
        , {{ metrics.perform_secondary_calculation(metric_name, grain, dimensions, calc_config) }} as {{ metrics.generate_secondary_calculation_alias(metric_name, calc_config, grain, true) }}

                {% endfor -%}

            {%- endfor -%}

        {%- else -%}

            {%- for calc_config in secondary_calculations %}
        , {{ metrics.perform_secondary_calculation(metric_tree.base_set, grain, dimensions, calc_config) }} as {{ metrics.generate_secondary_calculation_alias(base_set,calc_config, grain, false) }}

            {%- endfor -%}

        {%- endif %}

    from {% if metric_tree.full_set|length > 1 -%} joined_metrics {%- else -%} {{ metric_tree.base_set[0] }}__final {%- endif %}
)

{% endmacro %}
