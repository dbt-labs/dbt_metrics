
--TODO: Do we have a list of aggregations that we're supporting on day one? 
{%- macro gen_primary_metric_aggregate(aggregate, expression) -%}
    {{ return(adapter.dispatch('gen_primary_metric_aggregate', 'dbt_metrics')(aggregate, expression)) }}
{%- endmacro -%}

{%- macro default__gen_primary_metric_aggregate(aggregate, expression) -%}
    {%- if aggregate == 'count' -%}
        {{ return(adapter.dispatch('metric_count', 'dbt_metrics')(expression)) }}
    
    {%- elif aggregate == 'count_distinct' -%}
        {{ return(adapter.dispatch('metric_count_distinct', 'dbt_metrics')(expression)) }}
    
    {%- elif aggregate == 'average' -%}
        {{ return(adapter.dispatch('metric_average', 'dbt_metrics')(expression)) }}
    
    {%- elif aggregate == 'max' -%}
        {{ return(adapter.dispatch('metric_max', 'dbt_metrics')(expression)) }}
       
    {%- elif aggregate == 'min' -%}
        {{ return(adapter.dispatch('metric_min', 'dbt_metrics')(expression)) }}
    
    {%- elif aggregate == 'sum' -%}
        {{ return(adapter.dispatch('metric_sum', 'dbt_metrics')(expression)) }}

    {%- elif aggregate == 'expression' -%}
        {{ return(adapter.dispatch('metric_expression', 'dbt_metrics')(expression)) }}

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

{%- macro default__metric_expression(expression) -%}
        {{ expression }}
{%- endmacro -%}