{% macro rolling(aggregate, interval, alias, metric_list=[]) %}

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
    {% if metric_list is string %}
        {% set metric_list = [metric_list] %}
    {% endif %}

    {% do return ({
        "calculation": "rolling",
        "aggregate": aggregate,
        "interval": interval,
        "alias": alias,
        "metric_list": metric_list
        })
    %}
{% endmacro %}