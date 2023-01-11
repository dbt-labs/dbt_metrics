{% macro gen_base_query(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions, total_dimension_count) %}
    {{ return(adapter.dispatch('gen_base_query', 'metrics')(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions, total_dimension_count)) }}
{% endmacro %}

{% macro default__gen_base_query(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions, total_dimension_count) %}
        {# This is the "base" CTE which selects the fields we need to correctly 
        calculate the metric.  -#}
        select 
            {% if grain -%}
            cast(base_model.{{metric_dictionary.timestamp}} as date) as metric_date_day,
            calendar_table.date_{{ grain }} as date_{{grain}},
            calendar_table.date_day as window_filter_date,
                {% if secondary_calculations | length > 0 %}
                    {%- for period in relevant_periods %}
            calendar_table.date_{{ period }},
                    {% endfor -%}
                {%- endif -%}
            {%- endif %}
            {#- -#}
            {%- for dim in dimensions -%}
            base_model.{{ dim }},
            {%- endfor -%}
            {%- for calendar_dim in calendar_dimensions -%}
            calendar_table.{{ calendar_dim }},
            {%- endfor -%}
            {{ metrics.gen_property_to_aggregate(metric_dictionary, grain, dimensions, calendar_dimensions) }}
        from {{ metric_dictionary.metric_model }} base_model 
        {# -#}
        {%- if grain or calendar_dimensions|length > 0 -%}
        {{ metrics.gen_calendar_table_join(metric_dictionary, calendar_tbl) }} 
        {%- endif -%}
        {# #}
        where 1=1
        {#- -#}
        {{ metrics.gen_filters(metric_dictionary, start_date, end_date) }}

{%- endmacro -%}