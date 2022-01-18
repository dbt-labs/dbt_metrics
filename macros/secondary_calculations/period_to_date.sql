{% macro default__metric_secondary_calculations_period_to_date(metric_name, dims, config) %}
    {%- set calc_sql %}
        {{- adapter.dispatch('aggregate_primary_metric', 'metrics')(config.aggregate, metric_name) -}}
        over (
            partition by date_{{ config.period }}
            {% if dims -%}
                , {{ dims | join(", ") }}
            {%- endif %}
            order by period
            rows between unbounded preceding and current row
        )
    {%- endset %}

    {%- do return (calc_sql) %}
    
{% endmacro %}