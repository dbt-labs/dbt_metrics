{% macro default__secondary_calculation_rolling(metric_name, dimensions, calc_config) %}
    {% set calc_sql %}
        {{ adapter.dispatch('aggregate_primary_metric', 'metrics')(calc_config.aggregate, metric_name) }}
        over (
            {% if dimensions -%}
                partition by {{ dimensions | join(", ") }} 
            {% endif -%}
            order by period
            rows between {{ calc_config.interval - 1 }} preceding and current row
        )
    {% endset %}

    {% do return (calc_sql) %}

{% endmacro %}

{% macro rolling(aggregate, interval, alias) %}

    {% set missing_args = [] %}
    {% if not aggregate %} 
        {% set _ = missing_args.append("aggregate") %}
    {% endif %}
    {% if not interval %}
        {% set _ = missing_args.append("interval") %}
    {% endif %}
    {% if missing_args | length > 0 %}
        {% do exceptions.raise_compiler_error( missing_args | join(", ") ~ ' not provided to period_over_period') %}
    {% endif %}

    {% do return ({
        "calculation": "rolling",
        "aggregate": aggregate,
        "interval": interval,
        "alias": alias
        })
    %}
{% endmacro %}