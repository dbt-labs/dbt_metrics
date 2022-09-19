{% macro get_metric_allowlist() %}
    {{ return(adapter.dispatch('get_metric_allowlist', 'metrics')()) }}
{% endmacro %}

{% macro default__get_metric_allowlist() %}
    {# Keys are the primary aggregation, values are the permitted aggregations to run in secondary calculations. #}
    {% do return ({
        "average": ['min', 'max'],
        "count": ['min', 'max', 'sum', 'average'],
        "count_distinct": ['min', 'max', 'sum', 'average'],
        "sum": ['min', 'max', 'sum', 'average'],
        "max": ['min', 'max', 'sum', 'average'],
        "min": ['min', 'max', 'sum', 'average'],
        "derived": ['min', 'max', 'sum'],
    }) %}
{% endmacro %}