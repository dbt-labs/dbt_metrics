{%- macro get_window_interval(metric_window) %}

    {% set re = modules.re %}
    {% set number_pattern = '\d+' %}

    {% set search_object = re.search(number_pattern,metric_window) %}

    {% set interval = search_object[0] %}
    
    {% do return(interval) %}

{% endmacro -%}