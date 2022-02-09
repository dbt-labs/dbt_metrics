{% macro generate_secondary_calculation_alias(calc_config, grain) %}

    {{ return(adapter.dispatch('generate_secondary_calculation_alias', 'metrics')(calc_config, grain)) }}

{% endmacro %}

{% macro default__generate_secondary_calculation_alias(calc_config, grain) %}
    {% if calc_config.alias %}
        {% do return(calc_config.alias) %}
    {% endif %}
    
    {%- set calc_type = calc_config.calculation %}
    {%- if calc_type == 'period_over_period' %}
        {%- do return(calc_config.comparison_strategy ~ "_to_" ~ calc_config.interval ~ "_" ~ grain ~ "_ago") %}
   
    {%- elif calc_type == 'rolling' %}
        {%- do return("rolling_" ~ calc_config.aggregate ~ "_" ~ calc_config.interval ~ "_" ~ grain) %}
    
    {%- elif calc_type == 'period_to_date' %}
        {%- do return(calc_config.aggregate ~ "_for_" ~ calc_config.period) %}

    {%- else %}
        {%- do exceptions.raise_compiler_error("Can't generate alias for unknown secondary calculation: " ~ calc_type ~ ". calc_config: " ~ calc_config) %}  
    {%- endif %}

    {{- calc_sql }}
{% endmacro %}