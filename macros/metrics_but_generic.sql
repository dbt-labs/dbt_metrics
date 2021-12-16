/*
    Core metric query generation logic.
    TODO:
      - Lots of stuff!
      - account for adding timeseries calcs (eg. period-over-period change, Year-to-date, etc)
      - add support for defining filters (either in metric definition or in user query)
      - do something smarter about the date spine used below
      - think harder about how we actually calculate aggregates...
*/

{%- macro get_metric_sql(metric, grain, dims, calcs, metric_name) %}

with source_query as (

    select
        -- requested dimensions

        -- DEBUG: date_trunc(month, created_at)::date as month,
        -- DEBUG: Don't hard-code the time dimension? hmm....
        -- DEBUG: Need to cast as a date otherwise we get values like 2021-01-01 and 2021-01-01T00:00:00+00:00 that don't join :(
        date_trunc({{ grain }}, {{ metric.timestamp }})::date as period

        /*
            for dimensions on calendar table, we'll need to:
             - do an inequality join; something like signup_date >= date_day and signup_date < lead(1, date_day) over (order by date_day), or
             - date_trunc calendar table and source table separately, then equality join them before doing the first agg.
        */

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

 spine__time as (

     select distinct
        date_day as period
     from {{ ref('all_days_extended') }}

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

            , {{ metric_secondary_calculations(metric_name, metric.type, dims, calc) }} as calc_{{ loop.index }}

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