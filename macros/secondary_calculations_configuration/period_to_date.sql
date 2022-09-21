{% macro period_to_date(aggregate, period, alias, metric_list = []) %}

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
        "alias": alias,
        "metric_list": metric_list
        })
    %}
{% endmacro %}