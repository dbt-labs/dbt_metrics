{% macro gen_base_query(metrics_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, total_dimension_count, model_name, model_values) %}
    {{ return(adapter.dispatch('gen_base_query', 'metrics')(metrics_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, total_dimension_count, model_name, model_values)) }}
{% endmacro %}

{% macro default__gen_base_query(metrics_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, total_dimension_count, model_name, model_values) %}
        {# This is the "base" CTE which selects the fields we need to correctly 
        calculate the metric.  -#}
        select 
            {% if grain -%}
            {#- 
                Given that we've already determined the metrics in metric_names share
                the same windows & filters, we can base the conditional off of the first 
                value in the list because the order doesn't matter. 
            -#}
            cast(base_model.{{model_values.timestamp}} as date) as metric_date_day,
            calendar.date_{{ grain }} as date_{{grain}},
            calendar.date_day as window_filter_date,
                {%- if secondary_calculations | length > 0 %}
                    {%- for period in relevant_periods %}
            calendar.date_{{ period }},
                    {%- endfor -%}
                {%- endif -%}
            {%- endif -%}
            {#- -#}
            {%- for dim in dimensions %}
            base_model.{{ dim }},
            {%- endfor %}
            {%- for calendar_dim in calendar_dimensions -%}
            calendar.{{ calendar_dim }},
            {%- endfor -%}
            {%- for metric_name in model_values.metric_names -%}
            {{ metrics.gen_property_to_aggregate(metrics_dictionary[metric_name], grain, dimensions, calendar_dimensions) }}
            {%- if not loop.last -%},{%- endif -%}
            {%- endfor%}
        from {{ model_values.metric_model }} base_model 
        {# -#}
        {%- if grain or calendar_dimensions|length > 0 -%}
        {{ metrics.gen_calendar_join(model_values) }} 
        {%- endif -%}
        {# #}
        where 1=1
        {#- -#}
        {{ metrics.gen_filters(model_values, start_date, end_date) }}
        {# #}

{%- endmacro -%}