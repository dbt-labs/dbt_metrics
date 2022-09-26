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

            {% if grain != 'day' %}
            calendar_table.date_day as window_filter_date,
            {% endif %}

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
            1 as property_to_aggregate /*a specific expression to aggregate wasn't provided, so this effectively creates count(*) */
            {%- else -%}
                {%- do exceptions.raise_compiler_error("Expression to aggregate is required for non-count aggregation in metric `" ~ metric_dictionary.name ~ "`") -%}  
            {%- endif %}

        from {{ metric_dictionary.metric_model }} base_model 
        left join {{calendar_tbl}} calendar_table

        {% if metric_dictionary.window is not none %}
            on cast(base_model.{{metric_dictionary.timestamp}} as date) >= dateadd({{metric_dictionary.window.period}}, -{{metric_dictionary.window.count}}, calendar_table.date_day)
            and cast(base_model.{{metric_dictionary.timestamp}} as date) <= calendar_table.date_day
        {% else %}
            on cast(base_model.{{metric_dictionary.timestamp}} as date) = calendar_table.date_day
        {% endif %}

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

{% macro bigquery__gen_base_query(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions) %}

    {# This is the "base" CTE which selects the fields we need to correctly 
    calculate the metric.  #}
        select 

            {# This section looks at the sql aspect of the metric and ensures that 
            the value input into the macro is accurate -#}

            cast(base_model.{{metric_dictionary.timestamp}} as date) as metric_date_day, -- timestamp field
            
            {%- if grain != 'all_time'%}
            calendar_table.date_{{ grain }} as date_{{grain}},
            {% endif -%}

            {% if grain != 'day' %}
            calendar_table.date_day as window_filter_date,
            {% endif %}

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
            {%- elif metric_dictionary.calculation_method == 'count' -%}

            1 as property_to_aggregate /*a specific expression to aggregate wasn't provided, so this effectively creates count(*) */
            {%- else -%}
                {%- do exceptions.raise_compiler_error("Expression to aggregate is required for non-count aggregation in metric `" ~ metric_dictionary.name ~ "`") -%}  
            {%- endif %}

        from {{ metric_dictionary.metric_model }} base_model 
        left join {{calendar_tbl}} calendar_table

        {% if metric_dictionary.window is not none %}
            on cast(base_model.{{metric_dictionary.timestamp}} as date) >= date_sub(calendar_table.date_day, interval {{metric_dictionary.window.count}} {{metric_dictionary.window.period}})
            and cast(base_model.{{metric_dictionary.timestamp}} as date) <= calendar_table.date_day
        {% else %}
            on cast(base_model.{{metric_dictionary.timestamp}} as date) = calendar_table.date_day
        {% endif %}

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

{% macro postgres__gen_base_query(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions) %}

    {# This is the "base" CTE which selects the fields we need to correctly 
    calculate the metric.  #}
        select 
        
            {# This section looks at the sql aspect of the metric and ensures that 
            the value input into the macro is accurate -#}
            cast(base_model.{{metric_dictionary.timestamp}} as date) as metric_date_day, -- timestamp field
            
            {%- if grain != 'all_time'%}
            calendar_table.date_{{ grain }} as date_{{grain}},
            {% endif -%}
            
            {% if grain != 'day' %}
            calendar_table.date_day as window_filter_date,
            {% endif %}

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
            {%- elif metric_dictionary.calculation_method == 'count' -%}
            1 as property_to_aggregate /*a specific expression to aggregate wasn't provided, so this effectively creates count(*) */
            {%- else -%}
                {%- do exceptions.raise_compiler_error("Expression to aggregate is required for non-count aggregation in metric `" ~ metric_dictionary.name ~ "`") -%}  
            {%- endif %}

        from {{ metric_dictionary.metric_model }} base_model 
        left join {{calendar_tbl}} calendar_table

        {% if metric_dictionary.window is not none %}
            on cast(base_model.{{metric_dictionary.timestamp}} as date) >=  calendar_table.date_day - interval '{{metric_dictionary.window.count}} {{metric_dictionary.window.period}}'
            and cast(base_model.{{metric_dictionary.timestamp}} as date) <= calendar_table.date_day
        {% else %}
            on cast(base_model.{{metric_dictionary.timestamp}} as date) = calendar_table.date_day
        {% endif %}


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

{% macro redshift__gen_base_query(metric_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions) %}

    {# This is the "base" CTE which selects the fields we need to correctly 
    calculate the metric.  #}
        select 
        
            cast(base_model.{{metric_dictionary.timestamp}} as date) as metric_date_day, -- timestamp field
            
            {%- if grain != 'all_time'%}
            calendar_table.date_{{ grain }} as date_{{grain}},
            {% endif -%}

            {% if grain != 'day' %}
            calendar_table.date_day as window_filter_date,
            {% endif %}

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
            1 as property_to_aggregate /*a specific expression to aggregate wasn't provided, so this effectively creates count(*) */
            {%- else -%}
                {%- do exceptions.raise_compiler_error("Expression to aggregate is required for non-count aggregation in metric `" ~ metric_dictionary.name ~ "`") -%}  
            {%- endif %}

        from {{ metric_dictionary.metric_model }} base_model 
        left join {{calendar_tbl}} calendar_table

        {% if metric_dictionary.window is not none %}
            on cast(base_model.{{metric_dictionary.timestamp}} as date) >= dateadd({{metric_dictionary.window.period}}, -{{metric_dictionary.window.count}}, calendar_table.date_day)
            and cast(base_model.{{metric_dictionary.timestamp}} as date) <= calendar_table.date_day
        {% else %}
            on cast(base_model.{{metric_dictionary.timestamp}} as date) = calendar_table.date_day
        {% endif %}

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