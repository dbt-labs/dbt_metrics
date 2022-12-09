{% macro validate_secondary_calculations(metric_tree, metrics_dictionary, grain, secondary_calculations) %}


    {%- for metric_name in metric_tree.base_set %}
        {%- for calc_config in secondary_calculations if calc_config.aggregate -%}
            {%- do metrics.validate_aggregate_coherence(metric_aggregate=metrics_dictionary[metric_name].calculation_method, calculation_aggregate=calc_config.aggregate) -%}
        {%- endfor -%}
    {%- endfor -%}

    {%- for calc_config in secondary_calculations if calc_config.period -%}
        {%- do metrics.validate_grain_order(metric_grain=grain, calculation_grain=calc_config.period) -%}
    {%- endfor -%} 

{% endmacro %}