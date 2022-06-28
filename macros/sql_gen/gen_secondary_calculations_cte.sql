{% macro gen_secondary_calculation_cte(metric,dimensions,grain,all_metrics,secondary_calculations,calendar_dimensions) %}
    {{ return(adapter.dispatch('gen_secondary_calculation_cte', 'metrics')(metric,dimensions,grain,all_metrics,secondary_calculations,calendar_dimensions)) }}
{% endmacro %}

{% macro default__gen_secondary_calculation_cte(metric,dimensions,grain,all_metrics,secondary_calculations,calendar_dimensions) %}

{# The logic for secondary calculations is past the point where having calendar + dim
in a single list would create issues. So here we join them together. Plus it makes it
easier for not having to update the working secondary calc logic #}
{%- set dimensions = dimensions+calendar_dimensions -%}

,secondary_calculations as (

    select *

        {% for calc_config in secondary_calculations -%}
            , {{ metrics.perform_secondary_calculation(metric.name, grain, dimensions, calc_config) -}} as {{ metrics.generate_secondary_calculation_alias(calc_config, grain) }}

        {% endfor %}

    from 
        {% if all_metrics|length > 1 %} 
            joined_metrics
        {% else %} 
            {{metric.name}}__final
        {% endif %}
)

{% endmacro %}
