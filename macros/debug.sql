/*
    Small helper to look up a metric definition and call the macro
    to get the resulting SQL query. Sort of a stub for how we'd want
    to define / call metrics longer-term
    TODO:
    - Delete this?
*/
{% macro metric(metric_name, by, grain) %}
    {% set def = get_metric(metric_name) %}
    {% set sql = get_metric_sql(
        table = ref(def['table']),
        aggregate = def['aggregate'],
        expression = def['expression'],
        datetime = def['datetime'],

        grain = grain,
        dims = by
    ) %}
    {% do return(sql) %}
{% endmacro %}


/* -------------------------------------------------- */

/*
    For debugging, prints out a rendered query for a metric + params
    TODO:
    - Delete this?
*/
{% macro debug_metric(metric_name, by, grain) %}

    {% set query = metric(metric_name, by, grain) %}
    {% do log(query, info=True) %}
    {% set res = run_query(query) %}

    {% do res.print_table() %}

{% endmacro %}

{% macro get_metric(metric_name) %}
    {% if not execute %}
        {% do return(None) %}
    {% else %}
    {% set metric_info = namespace(metric_id=none) %}
    {% for metric in graph.metrics.values() %}
        {% if metric.name == metric_name %}
            {% set metric_info.metric_id = metric.unique_id %}
        {% endif %}
    {% endfor %}

    {% if metric_info.metric_id is none %}
        {% do exceptions.raise_compiler_error("Metric named '" ~ metric_name ~ "' not found") %}
    {% endif %}
    

    {% do return(graph.metrics[metric_info.metric_id]) %}
    {% endif %}

{% endmacro %}

{% macro debug(metric_name) %}
    {% if not execute %}
        {% do return("not execute") %}
    {% endif %}

    {% set metric = metrics.get_metric(metric_name) %}

    {%- set sql = metrics.get_metric_sql(
        metric,
        grain='month',
        dims=['has_messaged', 'is_weekend'],
        calcs=[
            {"type": "period_to_date", "aggregate": "sum", "period": "year", "alias": "ytd_sum"},
            {"type": "period_to_date", "aggregate": "max", "period": "month"},
            {"type": "period_over_period", "lag": 1, "how": "difference", "alias": "pop_1mth"},
            {"type": "period_over_period", "lag": 1, "how": "ratio"},
            {"type": "rolling", "window": 3, "aggregate": "average", "alias": "avg_3mth"},
            {"type": "rolling", "window": 3, "aggregate": "sum"},
        ]
    ) %}
    -- {# {% set res = run_query('select * from (' ~ sql ~ ') order by 2,1') %} #}
    -- {# {% do res.print_table(max_columns=none, max_rows=10) %} #}
    {{ sql }}

{% endmacro %}
