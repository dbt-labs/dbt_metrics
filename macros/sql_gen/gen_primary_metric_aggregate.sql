
{%- macro gen_primary_metric_aggregate(aggregate, expression, dimensions, grain) -%}
    {{ return(adapter.dispatch('gen_primary_metric_aggregate', 'metrics')(aggregate, expression, dimensions, grain)) }}
{%- endmacro -%}

{%- macro default__gen_primary_metric_aggregate(aggregate, expression, dimensions, grain) -%}

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
        {{ return(adapter.dispatch('metric_custom_sql', 'metrics')(aggregate, expression, dimensions, grain)) }}

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

{% macro default__metric_custom_sql(aggregate, expression, dimensions, grain) %}
    {% set sql_expression =  aggregate[13:] %}
        {% if '<<partition_by_dimensions>>' in sql_expression %}
            {% set split_parts = sql_expression.split("<<partition_by_dimensions>>") %}
            {% if dimensions == [] %}
                {%- set dim_expression = split_parts | join(" partition by true ") -%}
            {% elif dimensions != [] %}
                {%- set partition_by_expression = dimensions | join(', ') -%}
                {%- set dim_expression = split_parts | join(" partition by " ~ partition_by_expression) -%}
            {% endif %}
            {% set sql_expression = dim_expression %}
        {% endif %}
        {% if '<<partition_by_date>>' in sql_expression %}
            {% set split_parts = sql_expression.split("<<partition_by_date>>") %}
            {%- if grain is none -%}
                {%- set partition_by_time_expression = split_parts | join(" partition by true ") -%}
            {%- elif grain is not none -%}
                {%- set partition_by_time_expression = split_parts | join(" partition by " + "date_"+ grain) -%}
            {%- endif -%}
            {%- set sql_expression = partition_by_time_expression -%}
        {% endif %}
        {%- if ('<<order_by_date_asc>>' in sql_expression or '<<order_by_date_desc>>' in sql_expression) %}
            {% set order  = " asc " %}
            {% set split_string = "<<order_by_date_asc>>" %}
            {% if '<<order_by_date_desc>>' in sql_expression %}
                {% set order  = " desc " %}
                {% set split_string = "<<order_by_date_desc>>" %}
            {% endif %}
            {%- set split_parts = sql_expression.split(split_string) -%}
            {%- if grain is none -%}
                {%- set order_by_expression = " order by true " + order  -%}
                {%- set time_expression = split_parts | join(order_by_expression) -%}
            {%- elif grain is not none -%}
                {%- set order_by_expression = " order by " + "date_"+ grain + " " + order -%}
                {%- set time_expression = split_parts | join(order_by_expression) -%}
            {%- endif -%}
            {%- set sql_expression = time_expression -%}
            {%- endif -%}
    {{ sql_expression | replace("property_to_aggregate", expression) }}
{%- endmacro -%}