/*
    Core metric query generation logic.
    TODO:
      - add support for defining filters (either in metric definition or in user query)
      - validate that the requested dim is actually an option :rage:
      - provide fallback calendar table(?)
      - protect against nonsense secondary calculations (avg of avg, impossible grains)
*/


{%- macro get_metric_sql(metric, grain, dims, calcs) %}
{#/* TODO: This refs[0][0] stuff is totally ick */#}
{% set model = metrics.get_metric_relation(metric.refs[0] if execute else "") %}
{% set calendar_tbl = metrics.get_metric_calendar(var('metrics_calendar_table', "ref('all_days_extended')")) %}

-- TODO: Do I need to validate that the requested grain is defined on the metric?
{% for calc in calcs if calc.aggregate %}
    -- TODO: build a list of failures and return them all at once
    {% do metrics.validate_aggregate_coherence(metric.type, calc.aggregate) %}
{% endfor %}

{% for calc in calcs if calc.period %}
    -- TODO: build a list of failures and return them all at once
    {% do metrics.validate_grain_order(grain, calc.period) %}
{% endfor %}



with source_query as (

    select
        /* Always trunc to the day, then use dimensions on calendar table to achieve the _actual_ desired aggregates. */
        /* Need to cast as a date otherwise we get values like 2021-01-01 and 2021-01-01T00:00:00+00:00 that don't join :( */
        date_trunc(day, {{ metric.timestamp }})::date as date_day,

        {%- for dim in dims %}
            {%- if metrics.is_dim_from_model(metric, dim) %}
                 {{ dim }},
            {% endif -%}

        {% endfor %}

        {# /*When metric.sql is undefined or '*' for a count, 
            it's unnecessary to pull through the whole table */ #}
        {%- if metric.sql and metric.sql | trim != '*' -%}
            {{ metric.sql }} as property_to_aggregate
        {%- elif metric.type == 'count' -%}
            1 as property_to_aggregate /*a specific expression to aggregate wasn't provided, so this effectively creates count(*) */
        {%- else -%}
            {%- do exceptions.raise_compiler_error("Expression to aggregate is required for non-count aggregation in metric " ~ metric.name) -%}  
        {%- endif %}

    from {{ model }}
    where 1=1
    {%- for filter in metric.filters %}
        and {{ filter.field }} {{ filter.operator }} {{ filter.value }}
    {%- endfor %}
),

 spine__time as (
     select 
        date_day,
        
        /* this could be the same as date_day if grain is day. That's OK! 
        They're used for different things: date_day for joining to the spine, period for aggregating.*/
        date_{{ grain }} as period, 
        {{ dbt_utils.star(calendar_tbl, except=['date_day']) }}
     from {{ calendar_tbl }}

 ),

{%- for dim in dims -%}
    {%- if metrics.is_dim_from_model(metric, dim) %}
          
        spine__values__{{ dim }} as (

            select distinct {{ dim }}
            from source_query

        ),  
    {% endif -%}


{%- endfor %}

spine as (

    select *
    from spine__time
    {%- for dim in dims -%}

        {%- if metrics.is_dim_from_model(metric, dim) %}
            cross join spine__values__{{ dim }}
        {%- endif %}
    {%- endfor %}

),

{% set relevant_periods = [] %}
{% for calc in calcs if calc.period and calc.period not in relevant_periods %}
    {% set _ = relevant_periods.append(calc.period) %}
{% endfor %}

joined as (
    select 
        spine.period,
        {% for period in relevant_periods %}
        spine.date_{{ period }},
        {% endfor %}
        {% for dim in dims %}
        spine.{{ dim }},
        {% endfor %}

        {{- metrics.aggregate_primary_metric(metric.type, 'source_query.property_to_aggregate') }} as {{ metric.name }}

    from spine
    left outer join source_query on source_query.date_day = spine.date_day
    {% for dim in dims %}
        {% if metrics.is_dim_from_model(metric, dim) %}
            and source_query.{{ dim }} = spine.{{ dim }}
        {% endif %}
    {% endfor %}

    -- DEBUG: Add 1 twice to account for 1) timeseries dim and 2) to be inclusive of the last dim
    group by {{ range(1, (dims | length) + (relevant_periods | length) + 1 + 1) | join (", ") }}


),

with_calcs as (

    select *
        
        {% for calc in calcs -%}

            , {{ metrics.metric_secondary_calculations(metric.name, dims, calc) -}} as {{ metrics.secondary_calculation_alias(calc, grain) }}

        {% endfor %}

    from joined
    
)

select
    period
    {% for dim in dims %}
    , {{ dim }}
    {% endfor %}
    , coalesce({{ metric.name }}, 0) as {{ metric.name }}
    {% for calc in calcs %}
    , {{ metrics.secondary_calculation_alias(calc, grain) }}
    {% endfor %}

from with_calcs
order by {{ range(1, (dims | length) + 1 + 1) | join (", ") }}

{% endmacro %}