{% macro gen_calendar_join(model_values) %}
    {{ return(adapter.dispatch('gen_calendar_join', 'metrics')(model_values)) }}
{%- endmacro -%}

{% macro default__gen_calendar_join(model_values) %}
        left join calendar
        {%- if model_values.window is not none %}
            on cast(base_model.{{model_values.timestamp}} as date) > dateadd({{model_values.window.period}}, -{{model_values.window.count}}, calendar.date_day)
            and cast(base_model.{{metric_dictionary.timestamp}} as date) <= calendar.date_day
        {%- else %}
            on cast(base_model.{{model_values.timestamp}} as date) = calendar.date_day
        {% endif -%}
{% endmacro %}

{% macro bigquery__gen_calendar_join(model_values) %}
        left join calendar
        {%- if model_values.window is not none %}
            on cast(base_model.{{model_values.timestamp}} as date) > date_sub(calendar.date_day, interval {{model_values.window.count}} {{model_values.window.period}})
            and cast(base_model.{{model_values.timestamp}} as date) <= calendar.date_day
        {%- else %}
            on cast(base_model.{{model_values.timestamp}} as date) = calendar.date_day
        {% endif -%}
{% endmacro %}

{% macro postgres__gen_calendar_join(model_values) %}
        left join calendar
        {%- if model_values.window is not none %}
            on cast(base_model.{{model_values.timestamp}} as date) > calendar.date_day - interval '{{model_values.window.count}} {{model_values.window.period}}'
            and cast(base_model.{{model_values.timestamp}} as date) <= calendar.date_day
        {%- else %}
            on cast(base_model.{{model_values.timestamp}} as date) = calendar.date_day
        {% endif -%}
{% endmacro %}

{% macro redshift__gen_calendar_join(model_values) %}
        left join calendar
        {%- if model_values.window is not none %}
            on cast(base_model.{{model_values.timestamp}} as date) > dateadd({{model_values.window.period}}, -{{model_values.window.count}}, calendar.date_day)
            and cast(base_model.{{model_values.timestamp}} as date) <= calendar.date_day
        {%- else %}
            on cast(base_model.{{model_values.timestamp}} as date) = calendar.date_day
        {% endif -%}
{% endmacro %}
