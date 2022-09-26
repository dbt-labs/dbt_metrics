{%- macro gen_secondary_calculation_cte(metric_tree, grain, dimensions, secondary_calculations, calendar_dimensions, metric_dictionary
) -%}
    {{ return(adapter.dispatch('gen_secondary_calculation_cte', 'metrics')(metric_tree, grain, dimensions, secondary_calculations, calendar_dimensions, metric_dictionary)) }}
{%- endmacro -%}

{% macro default__gen_secondary_calculation_cte(metric_tree, grain, dimensions, secondary_calculations, calendar_dimensions, metric_dictionary) %}

{%- set metric_config = {} -%}
{%- for metric_name, data in metric_dictionary.items() -%}
    {%- do metric_config.update({metric_name: data.config}) -%}
{%- endfor -%}

{#- The logic for secondary calculations is past the point where having calendar + dim
in a single list would create issues. So here we join them together. Plus it makes it
easier for not having to update the working secondary calc logic -#}
{%- set dimensions = dimensions+calendar_dimensions -%}

, secondary_calculations as (

    select 
        *

        {%- for calc_config in secondary_calculations %}

            {# This step exists to only provide the limited list if that is provided #}
            {%- if calc_config.metric_list | length > 0 -%}

                {%- for metric_name in calc_config.metric_list %}
        , {{ metrics.perform_secondary_calculation(metric_name, grain, dimensions, calc_config, metric_config[metric_name]) }} as {{ metrics.generate_secondary_calculation_alias(metric_name,calc_config, grain, true) }}
                {%- endfor %}  

            {%- else %}

                {%- for metric_name in metric_tree.base_set %}
        , {{ metrics.perform_secondary_calculation(metric_name, grain, dimensions, calc_config, metric_config[metric_name]) }} as {{ metrics.generate_secondary_calculation_alias(metric_name,calc_config, grain, true) }}
                {%- endfor %}

            {%- endif %}

        {%- endfor %}


    from {% if metric_tree.full_set | length > 1 -%} joined_metrics {%- else -%} {{ metric_tree.base_set[0] }}__final {%- endif %}
)

{% endmacro %}
