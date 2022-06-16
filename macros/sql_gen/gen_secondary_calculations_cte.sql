{% macro gen_secondary_calculation_cte(metric,dimensions,grain,metric_list,secondary_calculations) %}
    {{ return(adapter.dispatch('gen_secondary_calculation_cte', 'metrics')(metric,dimensions,grain,metric_list,secondary_calculations)) }}
{% endmacro %}

{% macro default__gen_secondary_calculation_cte(metric,dimensions,grain,metric_list,secondary_calculations) %}

,secondary_calculations as (

    select *

        {% for calc_config in secondary_calculations -%}
            , {{ metrics.perform_secondary_calculation(metric.name, grain, dimensions, calc_config) -}} as {{ metrics.generate_secondary_calculation_alias(calc_config, grain) }}

        {% endfor %}

    from 
        {% if metric_list|length > 1 %} 
            joined_metrics
        {% else %} 
            {{metric.name}}__final
        {% endif %}
)

{% endmacro %}
