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


/*
    Small helper to return the metric subquery as a Selectable, ie.
    a thing that can be selected from, rather than a select statement itself
*/
{% macro query(metric_name, grain) %}
    {% set def = get_metric(metric_name) %}
    {% set sql = get_metric_sql(
        table = ref(def['table']),
        aggregate = def['aggregate'],
        expression = def['expression'],
        datetime = def['datetime'],

        grain = grain,
        dims = varargs
    ) %}

    {% set as_subquery %}
    (
        {{ sql }}
    )
    {% endset %}

    {% do return(as_subquery) %}
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

{% endmacro %}

{% macro debug(metric_name) %}

    {% set metric = get_metric(metric_name) %}

    {%- set sql = get_metric_sql(
        metric,
        grain='4_5_4_month',
        dims=['has_messaged', 'is_weekend'],
        calcs=[
            {"type": "period_to_date", "aggregate": "sum", "period": "year"},
            {"type": "period_to_date", "aggregate": "max", "period": "month"},
            {"type": "period_over_period", "lag": 1, "how": "difference"},
            {"type": "period_over_period", "lag": 1, "how": "ratio"},
            {"type": "rolling", "window": 3, "aggregate": "average"},
            {"type": "rolling", "window": 3, "aggregate": "sum"},
        ], 
        metric_name=metric.name
    ) %}
    -- {# {% set res = run_query('select * from (' ~ sql ~ ') order by 2,1') %} #}
    -- {# {% do res.print_table(max_columns=none, max_rows=10) %} #}
    {{ sql }}

{% endmacro %}

