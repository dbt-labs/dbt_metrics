{% macro gen_calendar_table_join(metric_dictionary, calendar_tbl) %}
    {{ return(adapter.dispatch('gen_calendar_table_join', 'metrics')(metric_dictionary, calendar_tbl)) }}
{% endmacro %}

{% macro default__gen_calendar_table_join(metric_dictionary, calendar_tbl) %}

    left join {{calendar_tbl}} calendar_table
    {% if metric_dictionary.window is not none %}
        on cast(base_model.{{metric_dictionary.timestamp}} as date) > dateadd({{metric_dictionary.window.period}}, -{{metric_dictionary.window.count}}, calendar_table.date_day)
        and cast(base_model.{{metric_dictionary.timestamp}} as date) <= calendar_table.date_day
    {% else %}
        on cast(base_model.{{metric_dictionary.timestamp}} as date) = calendar_table.date_day
    {% endif %}

{% endmacro %}

{% macro bigquery__gen_calendar_table_join(metric_dictionary, calendar_tbl) %}

    left join {{calendar_tbl}} calendar_table
    {% if metric_dictionary.window is not none %}
        on cast(base_model.{{metric_dictionary.timestamp}} as date) > date_sub(calendar_table.date_day, interval {{metric_dictionary.window.count}} {{metric_dictionary.window.period}})
        and cast(base_model.{{metric_dictionary.timestamp}} as date) <= calendar_table.date_day
    {% else %}
        on cast(base_model.{{metric_dictionary.timestamp}} as date) = calendar_table.date_day
    {% endif %}

{% endmacro %}

{% macro postgres__gen_calendar_table_join(metric_dictionary, calendar_tbl) %}

    left join {{calendar_tbl}} calendar_table
    {% if metric_dictionary.window is not none %}
        on cast(base_model.{{metric_dictionary.timestamp}} as date) > calendar_table.date_day - interval '{{metric_dictionary.window.count}} {{metric_dictionary.window.period}}'
        and cast(base_model.{{metric_dictionary.timestamp}} as date) <= calendar_table.date_day
    {% else %}
        on cast(base_model.{{metric_dictionary.timestamp}} as date) = calendar_table.date_day
    {% endif %}

{% endmacro %}

{% macro redshift__gen_calendar_table_join(metric_dictionary, calendar_tbl) %}

    left join {{calendar_tbl}} calendar_table
    {% if metric_dictionary.window is not none %}
        on cast(base_model.{{metric_dictionary.timestamp}} as date) > dateadd({{metric_dictionary.window.period}}, -{{metric_dictionary.window.count}}, calendar_table.date_day)
        and cast(base_model.{{metric_dictionary.timestamp}} as date) <= calendar_table.date_day
    {% else %}
        on cast(base_model.{{metric_dictionary.timestamp}} as date) = calendar_table.date_day
    {% endif %}

{% endmacro %}
