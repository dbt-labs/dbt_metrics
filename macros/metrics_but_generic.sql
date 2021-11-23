/*
    Core metric query generation logic.
    TODO:
      - Lots of stuff!
      - account for adding timeseries calcs (eg. period-over-period change, Year-to-date, etc)
      - add support for defining filters (either in metric definition or in user query)
      - do something smarter about the date spine used below
      - think harder about how we actually calculate aggregates...
*/

{% macro get_metric_sql(metric, grain, dims, calcs, metric_name='metric_value') %}

with source_query as (

    select
        -- requested dimensions

        -- DEBUG: date_trunc(month, created_at)::date as month,
        -- DEBUG: Don't hard-code the time dimension? hmm....
        -- DEBUG: Need to cast as a date otherwise we get values like 2021-01-01 and 2021-01-01T00:00:00+00:00 that don't join :(
        date_trunc({{ grain }}, {{ metric.timestamp }})::date as period

        {% for dim in dims %}
        , {{ dim }}
        {% endfor %}

        -- aggregate
        -- DEBUG: , count(*) as metric_value
        -- TODO : Handle count distinct
        , {{ aggregate_primary_metric(metric.type, metric.sql) }} as metric_value

    from {{ ref(metric.model) }}
    where 1=1
        -- via metric definition
        -- DEBUG: and customers.plan != 'free'

        -- user-supplied. Filters that are not present in the
        -- list of selected dimensions are applied at the source query
        -- DEBUG: and not customers.is_training_account
        -- DEBUG: Add 1 twice to account for 1) timeseries dim and 2) to be inclusive of the last dim
    group by {{ range(1, (dims | length) + 1 + 1) | join (", ") }}

),

-- DEBUG: This is a total hack - we'll need some sort of a days table...
 spine__time as (

     select distinct
         date_trunc({{ grain }}, dateadd(day, '-' || seq4(), current_date())) as period
     from table(generator(rowcount => 365))

 ),

{% for dim in dims %}

    spine__values__{{ dim }} as (

        select distinct {{ dim }}
        from source_query

    ),

{% endfor %}

spine as (

    select *
    from spine__time
    {% for dim in dims %}
    cross join spine__values__{{ dim }}
    {% endfor %}

),

joined as (

    select
        spine.period
        -- TODO : Exclude time grains that are finer grained
        --        than the specified time grain for the metric
        , date_trunc(day, spine.period) as period__day
        , date_trunc(week, spine.period) as period__week
        , date_trunc(month, spine.period) as period__month
        , date_trunc(quarter, spine.period) as period__quarter
        , date_trunc(year, spine.period) as period__year

        {% for dim in dims %}
        , spine.{{ dim }}
        {% endfor %}
        , metric_value as {{ metric_name }}

from spine
left join source_query on source_query.period = spine.period
{% for dim in dims %}
    and source_query.{{ dim }} = spine.{{ dim }}
{% endfor %}

),

with_calcs as (

    select *

        /*
            TODO:
                - Make sure denominators are all nonzero
                - Make sure division happens with floats, not ints
                - Wrap these expressions up in macros in this package
        */
        
        {% for calc in calcs %}

            , {{ secondary_calculations(metric_name, metric.type, dims, calc) }} as calc_{{ loop.index }}

        {% endfor %}

    from joined
    
)

select
    period
    {% for dim in dims %}
    , {{ dim }}
    {% endfor %}
    , coalesce({{ metric_name }}, 0) as {{ metric_name }}
    {% for calc in calcs %}
    , calc_{{ loop.index }}
    {% endfor %}

from with_calcs
order by {{ range(1, (dims | length) + 1 + 1) | join (", ") }}

{% endmacro %}

/* -------------------------------------------------- */

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

{% macro debug(metric_name) %}
    {% set metric_info = namespace(metric_id=none) %}
    {% for metric in graph.metrics.values() %}
        {% if metric.name == metric_name %}
            {% set metric_info.metric_id = metric.unique_id %}
        {% endif %}
    {% endfor %}

    {% if metric_info.metric_id is none %}
        {% do exceptions.raise_compiler_error("Metric named '" ~ metric_name ~ "' not found") %}
    {% endif %}
    

    {% set metric = graph.metrics[metric_info.metric_id] %}
    {% do log(metric, info=true) %}
    {% set sql = get_metric_sql(
        metric,
        grain='day',
        dims=['order_total_band'],
        calcs=[
            {"type": "period_over_period", "lag": 1, "how": "difference"},
            {"type": "period_over_period", "lag": 1, "how": "ratio"},
            {"type": "rolling", "window": 3, "aggregate": "average"},
            {"type": "rolling", "window": 3, "aggregate": "sum"},
            {"type": "period_to_date", "aggregate": "sum", "period": "year"},
            {"type": "period_to_date", "aggregate": "max", "period": "month"},
        ]
    ) %}
    {% set res = run_query('select * from (' ~ sql ~ ') order by 2,1') %}
    {% do res.print_table(max_columns=none, max_rows=10) %}

{% endmacro %}

