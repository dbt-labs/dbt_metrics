{% macro secondary_calculation_alias(grain, calc_config) %}

    {{ return(adapter.dispatch('secondary_calculation_alias', 'metrics')(grain, calc_config)) }}

{% endmacro %}

{% macro default__secondary_calculation_alias(calc_config, grain) %}
    {% if calc_config.alias %}
        {% do return(calc_config.alias) %}
    {% endif %}
    
    {%- set calc_type = calc_config.type %}
    {%- if calc_type == 'period_over_period' %}
        {%- do return(calc_config.how ~ "_to_" ~ calc_config.lag ~ "_" ~ grain ~ "_ago") %}
   
    {%- elif calc_type == 'rolling' %}
        {%- do return("rolling_" ~ calc_config.aggregate ~ "_" ~ calc_config.window ~ "_" ~ grain) %}
    
    {%- elif calc_type == 'period_to_date' %}
        {%- do return(calc_config.aggregate ~ "_for_" ~ calc_config.period) %}

    {%- else %}
        {%- do exceptions.raise_compiler_error("Can't generate alias for unknown secondary calculation: " ~ calc_type) %}  
    {%- endif %}

    {{- calc_sql }}
{% endmacro %}