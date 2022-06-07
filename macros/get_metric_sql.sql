/*
    Core metric query generation logic.
    TODO:
      - validate that the requested dim is actually an option (or fail at query execution instead of at compilation if they don't exist? is it a problem to expose columns that exist in the table but aren't "blessed" for the metric?)
      - allow start/end dates on metrics. Maybe special-case "today"?
      - allow passing in a seed with targets for a metric's value
*/


{%- macro get_metric_sql(metric, grain, dimensions, secondary_calculations, start_date, end_date, where) %}
{%- if not execute %}
    {%- do return("not execute") %}
{%- endif %}

{%- if not metric %}
    {%- do exceptions.raise_compiler_error("No metric provided") %}
{%- endif %}

{%- if not grain %}
    {%- do exceptions.raise_compiler_error("No date grain provided") %}
{%- endif %}

{#-/* TODO: This refs[0][0] stuff is totally ick */#}
{%- set model = metrics.get_metric_relation(metric.refs[0] if execute else "") %}
{%- set calendar_tbl = ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar")) %}

{#- /* TODO: Do I need to validate that the requested grain is defined on the metric? */ #}
{#- /* TODO: build a list of failures and return them all at once*/ #}
{%- for calc_config in secondary_calculations if calc_config.aggregate %}
    {%- do metrics.validate_aggregate_coherence(metric.type, calc_config.aggregate) %}
{%- endfor %}

{#- /* TODO: build a list of failures and return them all at once*/ #}
{%- for calc_config in secondary_calculations if calc_config.period %}
    {%- do metrics.validate_grain_order(grain, calc_config.period) %}
{%- endfor %}

{%- set relevant_periods = [] %}
{%- for calc_config in secondary_calculations if calc_config.period and calc_config.period not in relevant_periods %}
    {%- set _ = relevant_periods.append(calc_config.period) %}
{%- endfor -%}

with source_query as (

    select
        /* Always trunc to the day, then use dimensions on calendar table to achieve the _actual_ desired aggregates. */
        /* Need to cast as a date otherwise we get values like 2021-01-01 and 2021-01-01T00:00:00+00:00 that don't join :( */
        cast({{ dbt_utils.date_trunc('day', 'cast(' ~ metric.timestamp ~ ' as date)') }} as date) as date_day,
        
        {% for dim in dimensions %}
            {%- if metrics.is_dim_from_model(metric, dim) -%}
                 {{ dim }},
            {% endif -%}

        {%- endfor %}

        {#- /*When metric.sql is undefined or '*' for a count, 
            it's unnecessary to pull through the whole table */ #}
        {%- if metric.sql and metric.sql | replace('*', '') | trim != '' -%}
            {{ metric.sql }} as property_to_aggregate
        {%- elif metric.type == 'count' -%}
            1 as property_to_aggregate /*a specific expression to aggregate wasn't provided, so this effectively creates count(*) */
        {%- else -%}
            {%- do exceptions.raise_compiler_error("Expression to aggregate is required for non-count aggregation in metric `" ~ metric.name ~ "`") -%}  
        {%- endif %}

    from {{ model }}
    where 1=1
    {%- for filter in metric.filters %}
        and {{ filter.field }} {{ filter.operator }} {{ filter.value }}
    {%- endfor %}
    {%- for filter in where %}
        and {{ filter }}
    {%- endfor %}
),

spine__time as (
     select 
        /* this could be the same as date_day if grain is day. That's OK! 
        They're used for different things: date_day for joining to the spine, period for aggregating.*/
        date_{{ grain }} as period, 
        {% for period in relevant_periods %}
            date_{{ period }},
        {% endfor %}
        {% for dim in dimensions if not metrics.is_dim_from_model(metric, dim) %}
            {{ dim }},
        {% endfor %}
        date_day
     from {{ calendar_tbl }}

),

{% if dimensions is defined and dimensions|length > 0 %}
spine__values as (

    {#- /*This and the following CTEs were changed on 5/20 in order to remove 
    the cartesian join behaviour that resulted in impossible combinations of 
    data. */ #}
    select distinct 
        {%- for dim in dimensions -%}
            {%- if metrics.is_dim_from_model(metric, dim) %}
                {{ dim }}
                {% if not loop.last %},{% endif %}
            {% endif -%}
        {%- endfor %}
    from source_query

),  
{% endif -%}


spine as (

    select *
    from spine__time
    {% if dimensions is defined and dimensions|length > 0 %}
    cross join spine__values
    {% endif -%}

),

joined as (
    select 
        spine.period,
        {% for period in relevant_periods %}
        spine.date_{{ period }},
        {% endfor %}
        {% for dim in dimensions %}
        spine.{{ dim }},
        {% endfor %}

        -- has to be aggregated in this CTE to allow dimensions coming from the calendar table
        {{- metrics.aggregate_primary_metric(metric.type, 'source_query.property_to_aggregate') }} as {{ metric.name }},
        {{ dbt_utils.bool_or('source_query.date_day is not null') }} as has_data

    from spine
    left outer join source_query on source_query.date_day = spine.date_day
    {% for dim in dimensions %}
        {%- if metrics.is_dim_from_model(metric, dim) %}
            and (  source_query.{{ dim}} = spine.{{ dim }}
                or source_query.{{ dim }} is null and spine.{{ dim }} is null
            )
        {%- endif %}
    {% endfor %}

    {#- /* Add 1 twice to account for 1) timeseries dim and 2) to be inclusive of the last dim */ #}
    group by {{ range(1, (dimensions | length) + (relevant_periods | length) + 1 + 1) | join (", ") }}

),

bounded as (
    select 
        *,
        {% if start_date %}cast('{{ start_date }}' as date){% else %} min(case when has_data then period end) over () {% endif %} as lower_bound,
        {% if end_date %}cast('{{ end_date }}' as date){% else %} max(case when has_data then period end) over () {% endif %} as upper_bound
    from joined 
),

secondary_calculations as (

    select *
        
        {% for calc_config in secondary_calculations -%}

            , {{ metrics.perform_secondary_calculation(metric.name, dimensions, calc_config) -}} as {{ metrics.generate_secondary_calculation_alias(calc_config, grain) }}

        {% endfor %}

    from bounded
    
),

final as (
    select
        period
        {% for dim in dimensions %}
        , {{ dim }}
        {% endfor %}
        , coalesce({{ metric.name }}, 0) as {{ metric.name }}
        {% for calc_config in secondary_calculations %}
        , {{ metrics.generate_secondary_calculation_alias(calc_config, grain) }}
        {% endfor %}

    from secondary_calculations
    where period >= lower_bound
    and period <= upper_bound
    order by {{ range(1, (dimensions | length) + 1 + 1) | join (", ") }}
)

select * from final

{% endmacro %}