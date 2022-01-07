/*
    Core metric query generation logic.
    TODO:
      - Lots of stuff!
      - account for adding timeseries calcs (eg. period-over-period change, Year-to-date, etc)
      - add support for defining filters (either in metric definition or in user query)
      - do something smarter about the date spine used below
      - think harder about how we actually calculate aggregates...
      - validate that the requested dim is actually an option :rage:
*/

{% macro get_metric_relation(ref_name) %}
    {% if execute %}
        {% set model_ref_node = graph.nodes.values() | selectattr('name', 'equalto', ref_name) | first %}
        {% set relation = api.Relation.create(
            database = model_ref_node.database,
            schema = model_ref_node.schema,
            identifier = model_ref_node.alias
        )
        %}
        {% do return(relation) %}
    {% else %}
        {% do return(api.Relation.create()) %}
    {% endif %} 
{% endmacro %}

{%- macro get_metric_sql(metric, grain, dims, calcs, metric_name) %}
{% set model = metrics.get_metric_relation(metric.model) %}
{% set calendar_tbl = metrics.get_metric_relation(var('metrics_calendar_table', 'all_days_extended')) %}

with source_query as (

    select
        -- requested dimensions

        -- Always trunc to the day, then use dimensions on calendar table to achieve the _actual_ desired aggregates. 
        -- DEBUG: Don't hard-code the time dimension? hmm....
        -- DEBUG: Need to cast as a date otherwise we get values like 2021-01-01 and 2021-01-01T00:00:00+00:00 that don't join :(
        date_trunc(day, {{ metric.timestamp }})::date as date_day,

        {% for dim in dims %}
            {% if metrics.is_dim_from_model(metric, dim) %}
                 {{ dim }},
            {% endif %}

        {% endfor %}

        {{ metric.sql }} as property_to_aggregate

    from {{ model }}
    where 1=1
        -- via metric definition
        -- DEBUG: and customers.plan != 'free'

        -- user-supplied. Filters that are not present in the
        -- list of selected dimensions are applied at the source query
        -- DEBUG: and not customers.is_training_account
    {% for filter in metric.filters %}
        and {{ filter.field }} {{ filter.operator }} {{ filter.value }}
    {% endfor %}
),

 spine__time as (
     select 
        date_day,
        -- this could be the same as date_day if grain is day. That's OK! They're used for different things: date_day for joining to the spine, period for aggregating.
        date_{{ grain }} as period, 
        {{ dbt_utils.star(calendar_tbl, except=['date_'~ grain]) }}
     from {{ calendar_tbl }}

 ),

{% for dim in dims %}
    {% if metrics.is_dim_from_model(metric, dim) %}
          
        spine__values__{{ dim }} as (

            select distinct {{ dim }}
            from source_query

        ),  
    {% endif %}


{% endfor %}

spine as (

    select *
    from spine__time
    {% for dim in dims %}

        {% if metrics.is_dim_from_model(metric, dim) %}
            cross join spine__values__{{ dim }}
        {% endif %}
    {% endfor %}

),

joined as (
    select 
        spine.period,
        {% for dim in dims %}
        spine.{{ dim }},
        {% endfor %}

        -- TODO: distinct calcs periods (month/year)

        {{ metrics.aggregate_primary_metric(metric.type, 'source_query.property_to_aggregate') }} as {{ metric_name }}

    from spine
    left outer join source_query on source_query.date_day = spine.date_day
    {% for dim in dims %}
        {% if metrics.is_dim_from_model(metric, dim) %}
            and source_query.{{ dim }} = spine.{{ dim }}
        {% endif %}
    {% endfor %}

    -- DEBUG: Add 1 twice to account for 1) timeseries dim and 2) to be inclusive of the last dim
    group by {{ range(1, (dims | length) + 1 + 1) | join (", ") }}


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

            , {{ metrics.metric_secondary_calculations(metric_name, metric.type, dims, calc) }} as calc_{{ loop.index }}

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

{% macro is_dim_from_model(metric, dim_name) %}
    {% if execute %}

        {% set model_dims = metric['meta']['dimensions'][0]['columns'] %}
        {% do return (dim_name in model_dims) %}
    {% else %}
        {% do return (False) %}
    {% endif %}
{% endmacro %}