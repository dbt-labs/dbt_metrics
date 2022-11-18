{% macro generate_secondary_calculation_alias(metric_name, calc_config, grain, is_multiple_metrics) %}

    {{ return(adapter.dispatch('generate_secondary_calculation_alias', 'metrics')(metric_name, calc_config, grain, is_multiple_metrics)) }}

{% endmacro %}

{% macro default__generate_secondary_calculation_alias(metric_name, calc_config, grain, is_multiple_metrics) %}
    {%- if calc_config.alias -%}
        {%- if is_multiple_metrics -%}
            {%- do return(metric_name ~ "_" ~ calc_config.alias) -%}
        {%- else -%}
            {% do return(calc_config.alias) %}
        {%- endif -%}
    {%- endif -%}
    
    {%- set calc_type = calc_config.calculation -%}
    {%- if calc_type == 'period_over_period' -%}
        {%- if is_multiple_metrics -%}
            {%- do return(metric_name ~ "_" ~ calc_config.comparison_strategy ~ "_to_" ~ calc_config.interval ~ "_" ~ grain ~ "_ago") %}
        {%- else -%}
            {%- do return(calc_config.comparison_strategy ~ "_to_" ~ calc_config.interval ~ "_" ~ grain ~ "_ago") %}
        {%- endif -%}
   
    {%- elif calc_type == 'rolling' %}
        {%- if is_multiple_metrics -%}
            {%- if calc_config.interval -%}
                {%- do return(metric_name ~ "_" ~ "rolling_" ~ calc_config.aggregate ~ "_" ~ calc_config.interval ~ "_" ~ grain) %}
            {%- else -%}
                {%- do return(metric_name ~ "_" ~ "rolling_" ~ calc_config.aggregate) %}
            {%- endif -%}
        {%- else -%}
            {%- if calc_config.interval -%}
                {%- do return("rolling_" ~ calc_config.aggregate ~ "_" ~ calc_config.interval ~ "_" ~ grain) %}
            {%- else -%}
                {%- do return("rolling_" ~ calc_config.aggregate) %}
            {%- endif -%}
        {%- endif -%}
    
    {%- elif calc_type == 'period_to_date' %}
        {% if is_multiple_metrics %}
            {%- do return(metric_name ~ "_" ~ calc_config.aggregate ~ "_for_" ~ calc_config.period) %}
        {% else %}
            {%- do return(calc_config.aggregate ~ "_for_" ~ calc_config.period) %}
        {% endif %}
        
    {%- elif calc_type == 'prior' %}
        {% if is_multiple_metrics %}
            {%- do return(metric_name ~ "_" ~ calc_config.interval ~ "_" ~ grain ~ "s_prior") %}
        {% else %}
            {%- do return(calc_config.interval ~ "_" ~ grain ~ "s_prior") %}
        {% endif %}

    {%- else %}
        {%- do exceptions.raise_compiler_error("Can't generate alias for unknown secondary calculation: " ~ calc_type ~ ". calc_config: " ~ calc_config) %}  
    {%- endif %}

    {{ calc_sql }}
{% endmacro %}