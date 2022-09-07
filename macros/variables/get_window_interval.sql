{%- macro get_window_interval(metric_window) %}

    {% set re = modules.re %}
    {% set number_pattern = '\d+' %}

    {% set interval = re.search(number_pattern,metric_window) %}

    {% if 'day' in metric_window %}
        {% set bigquery_datepart = interval[0] ~ ' day'%}
    {% elif 'week' in metric_window %}
        {% set bigquery_datepart = interval[0] ~ ' week'%}
    {% elif 'month' in metric_window %}
        {% set bigquery_datepart = interval[0] ~ ' month'%}
    {% elif 'year' in metric_window %}
        {% set bigquery_datepart = interval[0] ~ ' year'%}
    {% endif %}
    
    {% do return(bigquery_datepart) %}

{% endmacro -%}