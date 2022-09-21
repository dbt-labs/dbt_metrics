{% macro default__secondary_calculation_rolling(metric_name, grain, dimensions, calc_config) %}
    {% set calc_sql %}
        {{ adapter.dispatch('gen_primary_metric_aggregate', 'metrics')(calc_config.aggregate, metric_name) }}
        over (
            {% if dimensions -%}
                partition by {{ dimensions | join(", ") }} 
            {% endif -%}
            order by date_{{grain}}
            rows between {{ calc_config.interval - 1 }} preceding and current row
        )
    {% endset %}

    {% do return (calc_sql) %}

{% endmacro %}