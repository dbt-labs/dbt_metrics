{% macro default__secondary_calculation_period_over_period(metric_name, dimensions, calc_config) %}
    {% set calc_sql %}
        lag(
            {{- metric_name }}, {{ calc_config.interval -}}
        ) over (
            {% if dimensions -%}
                partition by {{ dimensions | join(", ") }} 
            {% endif -%}
            order by period
        )
    {% endset %}
    

    {% if calc_config.comparison_strategy == 'difference' %}
        {% do return (adapter.dispatch('metric_comparison_strategy_difference', 'metrics')(metric_name, calc_sql)) %}
    
    {% elif calc_config.comparison_strategy == 'ratio' %}
        {% do return (adapter.dispatch('metric_comparison_strategy_ratio', 'metrics')(metric_name, calc_sql)) %}
    
    {% else %}
        {% do exceptions.raise_compiler_error("Bad comparison_strategy for period_over_period: " ~ calc_config.comparison_strategy ~ ". calc_config: " ~ calc_config) %}
    {% endif %}

{% endmacro %}

{% macro default__metric_comparison_strategy_difference(metric_name, calc_sql) %}
    coalesce({{ metric_name }}, 0) - coalesce({{ calc_sql }}, 0)
{% endmacro %}

{% macro default__metric_comparison_strategy_ratio(metric_name, calc_sql) %}
    cast(coalesce({{ metric_name }}, 0) / nullif({{ calc_sql }}, 0) as {{ dbt_utils.type_float() }})
{% endmacro %}

{% macro period_over_period(comparison_strategy, interval, alias) %}

    {% set missing_args = [] %}
    {% if not comparison_strategy %}
        {% set _ = missing_args.append("comparison_strategy") %}
    {% endif %}
    {% if not interval %} 
        {% set _ = missing_args.append("interval") %}
    {% endif %}
    {% if missing_args | length > 0 %}
        {% do exceptions.raise_compiler_error( missing_args | join(", ") ~ ' not provided to period_over_period') %}
    {% endif %}

    {% do return ({
        "calculation": "period_over_period",
        "comparison_strategy": comparison_strategy,
        "interval": interval,
        "alias": alias
        })
    %}
{% endmacro %}