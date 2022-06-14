{% macro gen_base_query(metric,model,grain,dimensions,start_date, end_date, where, calendar_tbl) %}
    {{ return(adapter.dispatch('gen_base_query', 'metrics')(metric,model,grain,dimensions,start_date, end_date, where, calendar_tbl)) }}
{% endmacro %}

{% macro default__gen_base_query(metric,model,grain,dimensions,start_date, end_date, where, calendar_tbl) %}

    {# This is the "base" CTE which selects the fields we need to correctly 
    calculate the metric.  #}
    select 
        {# This section looks at the sql aspect of the metric and ensures that 
        the value input into the macro is accurate #}
        to_date({{metric.timestamp}}) as metric_date_day, -- timestamp field
        {{calendar_tbl}}.date_{{ grain }} as date_{{grain}},
        -- ALL DIMENSIONS
        {% for dim in dimensions %}
            {%- if metrics.is_dim_from_model(metric, dim) -%}
                {{ dim }},
            {% endif -%}
        {%- endfor %}
        {%- if metric.sql and metric.sql | replace('*', '') | trim != '' -%}
            {{ metric.sql }} as property_to_aggregate
        {%- elif metric.type == 'count' -%}
            1 as property_to_aggregate /*a specific expression to aggregate wasn't provided, so this effectively creates count(*) */
        {%- else -%}
            {%- do exceptions.raise_compiler_error("Expression to aggregate is required for non-count aggregation in metric `" ~ metric.name ~ "`") -%}  
        {%- endif %}
    from {{ model }}
    left join {{calendar_tbl}} on to_date({{metric.timestamp}}) = date_day
    where 1=1
    
    -- metric start/end dates also applied here to limit incoming data
    {% if start_date or end_date%}
        and (
        {% if start_date and end_date %}
            {{metric.timestamp}} >= cast('{{ start_date }}' as date)
            and {{metric.timestamp}} <= cast('{{ end_date }}' as date)
        {% elif start_date and not end_date %}
            {{metric.timestamp}} >= cast('{{ start_date }}' as date)
        {% elif end_date and not start_date %}
            {{metric.timestamp}} <= cast('{{ end_date }}' as date)
        {% endif %} 
        )      
    {% endif %} 

    -- metric where clauses...
    {% if metric.filters %}
    and (
        {%- for filter in metric.filters %}
            {{ filter.field }} {{ filter.operator }} {{ filter.value }}
            {% if not loop.last %} and {% endif %}
        {%- endfor %}
    )
    {% endif%}


    -- metric where clauses...
    {% if where %}
    and (
        {%- for filter in where %}
            {{ filter }}
            {% if not loop.last %} and {% endif %}
        {%- endfor %}
    )
    {% endif %}

{% endmacro %}
