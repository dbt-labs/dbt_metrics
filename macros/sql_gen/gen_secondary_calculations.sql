{%- macro gen_secondary_calculations(metric_tree, metrics_dictionary, grain, dimensions, secondary_calculations, calendar_dimensions) -%}
    {{ return(adapter.dispatch('gen_secondary_calculations', 'metrics')(metric_tree, metrics_dictionary, grain, dimensions, secondary_calculations, calendar_dimensions)) }}
{%- endmacro -%}

{% macro default__gen_secondary_calculations(metric_tree, metrics_dictionary, grain, dimensions, secondary_calculations, calendar_dimensions) %}

{%- for calc_config in secondary_calculations %}
    {%- if calc_config.metric_list | length > 0 -%}
        {%- for metric_name in calc_config.metric_list -%}
    ,{{ metrics.perform_secondary_calculation(metric_name, grain, dimensions, calendar_dimensions, calc_config, metrics_dictionary[metric_name].config) }}
        {%- endfor %}  
    {%- else %}
        {%- for metric_name in metric_tree.base_set -%}
    , {{ metrics.perform_secondary_calculation(metric_name, grain, dimensions, calendar_dimensions, calc_config, metrics_dictionary[metric_name].config) }}
        {%- endfor %}
    {%- endif %}
{%- endfor %}

{%- endmacro %}
