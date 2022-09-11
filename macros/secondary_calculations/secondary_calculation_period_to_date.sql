{% macro default__secondary_calculation_period_to_date(metric_name, grain, dimensions, calc_config) %}
    {%- set calc_sql -%}
        {{- adapter.dispatch('gen_primary_metric_aggregate', 'dbt_metrics')(calc_config.aggregate, metric_name) -}}
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

{% macro period_to_date(aggregate, period, alias) %}

    {% set missing_args = [] %}
    {% if not aggregate %} 
        {% set _ = missing_args.append("aggregate") %}
    {% endif %}
    {% if not period %}
        {% set _ = missing_args.append("period") %}
    {% endif %}
    {% if missing_args | length > 0 %}
        {% do exceptions.raise_compiler_error( missing_args | join(", ") ~ ' not provided to period_to_date') %}
    {% endif %}

    {% do return ({
        "calculation": "period_to_date",
        "aggregate": aggregate,
        "period": period,
        "alias": alias
        })
    %}
{% endmacro %}