{% macro gen_base_query(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions) %}
    {{ return(adapter.dispatch('gen_base_query', 'metrics')(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions)) }}
{% endmacro %}

{% macro default__gen_base_query(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions) %}

    {# This is the "base" CTE which selects the fields we need to correctly 
    calculate the metric.  #}
        select 
        
            cast(base_model.{{metric_dictionary.timestamp}} as date) as metric_date_day, -- timestamp field
            
            {%- if grain != 'all_time'%}
            calendar_table.date_{{ grain }} as date_{{grain}},
            {% endif -%}

            calendar_table.date_day as window_filter_date,

            {% if secondary_calculations | length > 0 -%}
                {%- for period in relevant_periods %}
            calendar_table.date_{{ period }},
                {% endfor -%}
            {%- endif -%}

            {%- for dim in dimensions %}
                base_model.{{ dim }},
            {%- endfor %}

            {%- for calendar_dim in calendar_dimensions %}
                calendar_table.{{ calendar_dim }},
            {%- endfor %}

            {%- if metric_dictionary.expression and metric_dictionary.expression | replace('*', '') | trim != '' %}
                ({{ metric_dictionary.expression }}) as property_to_aggregate
            {%- elif metric_dictionary.calculation_type == 'count' -%}
            {# We use 1 as the property to aggregate in count so that it matches count(*) #}
            1 as property_to_aggregate 
            {%- else -%}
                {%- do exceptions.raise_compiler_error("Expression to aggregate is required for non-count aggregation in metric `" ~ metric_dictionary.name ~ "`") -%}  
            {%- endif %}

        from {{ metric_dictionary.metric_model }} base_model 
        {{ metrics.gen_calendar_table_join(metric_dictionary, calendar_tbl) }} 

        where 1=1
        
        {#- metric start/end dates also applied here to limit incoming data -#}
        {% if start_date or end_date %}
            and (
            {% if start_date and end_date -%}
                cast({{metric_dictionary.timestamp}} as date) >= cast('{{ start_date }}' as date)
                and cast({{metric_dictionary.timestamp}} as date) <= cast('{{ end_date }}' as date)
            {%- elif start_date and not end_date -%}
                cast({{metric_dictionary.timestamp}} as date) >= cast('{{ start_date }}' as date)
            {%- elif end_date and not start_date -%}
                cast({{metric_dictionary.timestamp}} as date) <= cast('{{ end_date }}' as date)
            {%- endif %} 
            )
        {% endif -%} 
        {#- metric filter clauses... -#}
        {% if metric_dictionary.filters %}
            and (
                {% for filter in metric_dictionary.filters -%}
                    {%- if not loop.first -%} and {% endif %}{{ filter.field }} {{ filter.operator }} {{ filter.value }}
                {% endfor -%}
            )
        {% endif -%}

{%- endmacro -%}