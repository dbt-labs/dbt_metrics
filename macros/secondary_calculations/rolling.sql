{% macro default__metric_secondary_calculations_rolling(metric_name, dims, config) %}
    {% set calc_sql %}
        {{ adapter.dispatch('aggregate_primary_metric', 'metrics')(config.aggregate, metric_name) }}
        over (
            {% if dims -%}
                partition by {{ dims | join(", ") }} 
            {% endif -%}
            order by period
            rows between {{ config.window - 1 }} preceding and current row
        )
    {% endset %}

    {% do return (calc_sql) %}

{% endmacro %}