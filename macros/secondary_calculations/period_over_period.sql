{% macro default__metric_secondary_calculations_period_over_period(metric_name, dims, config) %}
    {% set calc_sql %}
        lag(
            {{- metric_name }}, {{ config.lag -}}
        ) over (
            {% if dims -%}
                partition by {{ dims | join(", ") }} 
            {% endif -%}
            order by period
        )
    {% endset %}
    

    {% if config.how == 'difference' %}
        {% do return (adapter.dispatch('metric_how_difference', 'metrics')(metric_name, calc_sql)) %}
    
    {% elif config.how == 'ratio' %}
        {% do return (adapter.dispatch('metric_how_ratio', 'metrics')(metric_name, calc_sql)) %}
    
    {% else %}
        {% do exceptions.raise_compiler_error("Bad 'how' for period_over_period: " ~ config.how) %}
    {% endif %}

{% endmacro %}

{% macro default__metric_how_difference(metric_name, calc_sql) %}
    coalesce({{ metric_name }}, 0) - coalesce({{ calc_sql }}, 0)
{% endmacro %}

{% macro default__metric_how_ratio(metric_name, calc_sql) %}
    coalesce({{ metric_name }}, 0) / nullif({{ calc_sql }}, 0)::float
{% endmacro %}
