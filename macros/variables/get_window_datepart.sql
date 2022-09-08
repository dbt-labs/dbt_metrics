{%- macro get_window_datepart(metric_window) %}

    {% if 'day' in metric_window %}
        {% set datepart = ' day'%}
    {% elif 'week' in metric_window %}
        {% set datepart = ' week'%}
    {% elif 'month' in metric_window %}
        {% set datepart = ' month'%}
    {% elif 'year' in metric_window %}
        {% set datepart = ' year'%}
    {% endif %}
    
    {% do return(datepart) %}

{% endmacro -%}