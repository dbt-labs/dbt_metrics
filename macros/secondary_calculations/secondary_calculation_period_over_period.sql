{%- macro default__secondary_calculation_period_over_period(metric_name, grain, dimensions, calc_config, metric_config_dict) -%}
    {%- set calc_sql %}
            lag(
                {{ metric_name }}, {{ calc_config.interval }}
            ) over (
                {% if dimensions -%}
                    partition by {{ dimensions | join(", ") }} 
                {% endif -%}
                order by date_{{grain}}
            )
    {%- endset-%}
    
    {%- if calc_config.comparison_strategy == 'difference' -%}
        {% do return (adapter.dispatch('metric_comparison_strategy_difference', 'metrics')(metric_name, calc_sql, metric_config_dict)) %}
    
    {%- elif calc_config.comparison_strategy == 'ratio' -%}
        {% do return (adapter.dispatch('metric_comparison_strategy_ratio', 'metrics')(metric_name, calc_sql, metric_config_dict)) %}
    
    {-% else -%}
        {% do exceptions.raise_compiler_error("Bad comparison_strategy for period_over_period: " ~ calc_config.comparison_strategy ~ ". calc_config: " ~ calc_config) %}
    {%- endif -%}

{% endmacro %}

{% macro default__metric_comparison_strategy_difference(metric_name, calc_sql, metric_config_dict) -%}
    {%- if metric_config_dict.get("default_value_null", False) %}
        {{ metric_name }} - {{ calc_sql }}
    {%- else -%}
        coalesce({{ metric_name }}, 0) - coalesce(
        {{ calc_sql }}
        , 0)
    {%- endif %}
        
{%- endmacro -%}

{% macro default__metric_comparison_strategy_ratio(metric_name, calc_sql, metric_config_dict) -%}
    
    {%- if metric_config_dict.get("default_value_null", False) %}
        cast({{ metric_name }} as {{ type_float() }}) / nullif(
            {{ calc_sql }}
            , 0)
    {%- else -%}
         coalesce(
            cast({{ metric_name }} as {{ type_float() }}) / nullif(
            {{ calc_sql }}
            , 0) 
        , 0)
    {%- endif %}
    
{%- endmacro %}
