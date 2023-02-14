
{%- macro gen_primary_metric_aggregate(aggregate, expression, dimensions) -%}
    {{ return(adapter.dispatch('gen_primary_metric_aggregate', 'metrics')(aggregate, expression, dimensions)) }}
{%- endmacro -%}

{%- macro default__gen_primary_metric_aggregate(aggregate, expression, dimensions) -%}

    {%- if aggregate == 'count' -%}
        {{ return(adapter.dispatch('metric_count', 'metrics')(expression)) }}
    
    {%- elif aggregate == 'count_distinct' -%}
        {{ return(adapter.dispatch('metric_count_distinct', 'metrics')(expression)) }}
    
    {%- elif aggregate == 'average' -%}
        {{ return(adapter.dispatch('metric_average', 'metrics')(expression)) }}
    
    {%- elif aggregate == 'max' -%}
        {{ return(adapter.dispatch('metric_max', 'metrics')(expression)) }}
       
    {%- elif aggregate == 'min' -%}
        {{ return(adapter.dispatch('metric_min', 'metrics')(expression)) }}
    
    {%- elif aggregate == 'sum' -%}
        {{ return(adapter.dispatch('metric_sum', 'metrics')(expression)) }}

    {%- elif aggregate == 'median' -%}
        {{ return(adapter.dispatch('metric_median', 'metrics')(expression)) }}

    {%- elif aggregate == 'derived' -%}
        {{ return(adapter.dispatch('metric_derived', 'metrics')(expression)) }}
    
    {%- elif aggregate[:10] == 'custom_sql' -%}
        {{ return(adapter.dispatch('metric_custom_sql', 'metrics')(aggregate, expression, dimensions)) }}

    {%- else -%}
        {%- do exceptions.raise_compiler_error("Unknown aggregation style: " ~ aggregate) -%}  
    {%- endif -%}
{%- endmacro -%}

{% macro default__metric_count(expression) %}
        count({{ expression }})
{%- endmacro -%}

{% macro default__metric_count_distinct(expression) %}
        count(distinct {{ expression }})
{%- endmacro -%}

{% macro default__metric_average(expression) %}
        avg({{ expression }})
{%- endmacro -%}

{% macro redshift__metric_average(expression) %}
        avg(cast({{ expression }} as float))
{%- endmacro -%}

{% macro default__metric_max(expression) %}
        max({{ expression }})
{%- endmacro -%}

{% macro default__metric_min(expression) %}
        min({{ expression }})
{%- endmacro -%}

{% macro default__metric_sum(expression) %}
        sum({{ expression }})
{%- endmacro -%}

{% macro default__metric_median(expression) %}
        median({{ expression }})
{%- endmacro -%}

{% macro bigquery__metric_median(expression) %}
        any_value({{ expression }})
{%- endmacro -%}

{% macro postgres__metric_median(expression) %}
        percentile_cont(0.5) within group (order by {{ expression }})
{%- endmacro -%}

{% macro default__metric_derived(expression) %}
        {{ expression }}
{%- endmacro -%}

{% macro default__metric_custom_sql(aggregate, expression, dimensions) %}
        {% if '<<dimensions>>' in aggregate[13:] %}
            {% set first_part = aggregate[13:].split("<<dimensions>>")[0] %}
            {% set second_part = aggregate[13:].split("<<dimensions>>")[1] %}
            {{first_part.replace('property_to_aggregate', expression)}} {% for dimension in dimensions %} {{dimension}} {% if not loop.last %} , {% endif %} {% endfor %} {{second_part.replace('property_to_aggregate', expression)}}
        {% else %}
            {{aggregate[13:].replace('property_to_aggregate', expression)}}
        {%endif%}
{%- endmacro -%}