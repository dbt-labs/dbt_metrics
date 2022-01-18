{% macro metric_secondary_calculations(metric_name, dims, config) %}
    {{ return(adapter.dispatch('metric_secondary_calculations', 'metrics')(metric_name, dims, config)) }}
{% endmacro %}

{% macro default__metric_secondary_calculations(metric_name, dims, config) %}
    {%- set calc_type = config.type %}
    {%- set calc_sql = '' %}
    
    {%- if calc_type == 'period_over_period' %}
        {%- set calc_sql = adapter.dispatch('metric_secondary_calculations_period_over_period', 'metrics')(metric_name, dims, config) %}
   
    {%- elif calc_type == 'rolling' %}
        {%- set calc_sql = adapter.dispatch('metric_secondary_calculations_rolling', 'metrics')(metric_name, dims, config) %}
    
    {%- elif calc_type == 'period_to_date' %}
        {%- set calc_sql = adapter.dispatch('metric_secondary_calculations_period_to_date', 'metrics')(metric_name, dims, config) %}
    
    {%- else %}
        {%- do exceptions.raise_compiler_error("Unknown secondary calculation: " ~ calc_type) %}  
    {%- endif %}

    {{- calc_sql }}

{% endmacro %}