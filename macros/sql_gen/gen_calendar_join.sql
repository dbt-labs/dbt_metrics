{% macro gen_calendar_join(group_values) %}
    {{ return(adapter.dispatch('gen_calendar_join', 'metrics')(group_values)) }}
{%- endmacro -%}

{% macro default__gen_calendar_join(group_values) %}
        left join calendar
        {%- if group_values.window is not none %}
            on cast(base_model.{{group_values.timestamp}} as date) > dateadd({{group_values.window.period}}, -{{group_values.window.count}}, calendar.date_day)
            and cast(base_model.{{group_values.timestamp}} as date) <= calendar.date_day
        {%- else %}
            on cast(base_model.{{group_values.timestamp}} as date) = calendar.date_day
        {% endif -%}
{% endmacro %}

{% macro bigquery__gen_calendar_join(group_values) %}
        left join calendar
        {%- if group_values.window is not none %}
            on cast(base_model.{{group_values.timestamp}} as date) > date_sub(calendar.date_day, interval {{group_values.window.count}} {{group_values.window.period}})
            and cast(base_model.{{group_values.timestamp}} as date) <= calendar.date_day
        {%- else %}
            on cast(base_model.{{group_values.timestamp}} as date) = calendar.date_day
        {% endif -%}
{% endmacro %}

{% macro postgres__gen_calendar_join(group_values) %}
        left join calendar
        {%- if group_values.window is not none %}
            on cast(base_model.{{group_values.timestamp}} as date) > calendar.date_day - interval '{{group_values.window.count}} {{group_values.window.period}}'
            and cast(base_model.{{group_values.timestamp}} as date) <= calendar.date_day
        {%- else %}
            on cast(base_model.{{group_values.timestamp}} as date) = calendar.date_day
        {% endif -%}
{% endmacro %}

{% macro redshift__gen_calendar_join(group_values) %}
        left join calendar
        {%- if group_values.window is not none %}
            on cast(base_model.{{group_values.timestamp}} as date) > dateadd({{group_values.window.period}}, -{{group_values.window.count}}, calendar.date_day)
            and cast(base_model.{{group_values.timestamp}} as date) <= calendar.date_day
        {%- else %}
            on cast(base_model.{{group_values.timestamp}} as date) = calendar.date_day
        {% endif -%}
{% endmacro %}
