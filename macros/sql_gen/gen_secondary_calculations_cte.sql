{% macro gen_secondary_calculation_cte(base_set,dimensions,grain,full_set,secondary_calculations,calendar_dimensions) %}
    {{ return(adapter.dispatch('gen_secondary_calculation_cte', 'metrics')(base_set,dimensions,grain,full_set,secondary_calculations,calendar_dimensions)) }}
{% endmacro %}

{% macro default__gen_secondary_calculation_cte(base_set,dimensions,grain,full_set,secondary_calculations,calendar_dimensions) %}

{# The logic for secondary calculations is past the point where having calendar + dim
in a single list would create issues. So here we join them together. Plus it makes it
easier for not having to update the working secondary calc logic #}
{%- set dimensions = dimensions+calendar_dimensions -%}

,secondary_calculations as (

    select *

        {# {% set is_multiple_metrics = base_set | length > 1 %} #}
        {% for calc_config in secondary_calculations -%}
            {% if calc_config.metric_list | length > 0 %}
                {% for metric_name in calc_config.metric_list -%}
                    , {{ metrics.perform_secondary_calculation(metric_name, grain, dimensions, calc_config) -}} as {{ metrics.generate_secondary_calculation_alias(metric_name,calc_config, grain, true) }}
                {% endfor %}    
            {% else %}
                {% for metric_name in base_set -%}
                    , {{ metrics.perform_secondary_calculation(metric_name, grain, dimensions, calc_config) -}} as {{ metrics.generate_secondary_calculation_alias(metric_name,calc_config, grain, true) }}
                {% endfor %}
            {% endif%}
        
        {% endfor %}



    from 
        {% if full_set|length > 1 %} 
            joined_metrics
        {% else %} 
            {{ base_set[0] }}__final
        {% endif %}
)

{% endmacro %}
