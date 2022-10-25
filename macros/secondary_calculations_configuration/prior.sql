{% macro prior(interval, alias, metric_list = []) %}

    {% set missing_args = [] %}
    {% if not interval %} 
        {% set _ = missing_args.append("interval") %}
    {% endif %}
    {% if missing_args | length > 0 %}
        {% do exceptions.raise_compiler_error( missing_args | join(", ") ~ ' not provided to prior') %}
    {% endif %}
    {% if metric_list is string %}
        {% set metric_list = [metric_list] %}
    {% endif %}

    {% do return ({
        "calculation": "prior",
        "interval": interval,
        "alias": alias,
        "metric_list": metric_list
        })
    %}
{% endmacro %}