{% macro default__secondary_calculation_period_to_date(metric_name, grain, dimensions, calc_config) %}
    {%- set calc_sql -%}
        {{- adapter.dispatch('gen_primary_metric_aggregate', 'metrics')(calc_config.aggregate, metric_name) -}}
        over (
            partition by date_{{ calc_config.period }}
            {% if dimensions -%}
                , {{ dimensions | join(", ") }}
            {%- endif %}
            order by date_{{grain}}
            rows between unbounded preceding and current row
        )
    {%- endset %}

    {%- do return (calc_sql) %}
    
{% endmacro %}