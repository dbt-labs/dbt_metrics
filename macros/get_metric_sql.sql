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

{# This is awful. We have the metric.model information which returns the model name
in the ref function but in order to get JUST the model name, we have to parse through
the list of references. Additionally, the refs is a list of lists so we ALSO have to
parse through that list as well. #}

{# Awful as this is, it MIGHT be neccesary for when we have multiple metrics being 
built off of one another. We'll need to loop through the list of metrics and get
the relation for each one. #}
{%- set model = metrics.get_metric_relation(metric.refs[0][0] if execute else "") %}
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

with calendar as (

    {# This CTE creates our base calendar and then limits the date range for the 
    start and end date provided by the macro call #}
    select * 
    from {{ calendar_tbl }}
    {% if start_date or end_date%}
        {% if start_date and end_date %}
            where date_day >= cast('{{ start_date }}' as date)
            and date_day <= cast('{{ end_date }}' as date)
        {% elif start_date and not end_date %}
            where date_day >= cast('{{ start_date }}' as date)
        {% elif end_date and not start_date %}
            where date_day <= cast('{{ end_date }}' as date)
        {% endif %}       
    {% endif %} 

)
,
{{metric.name}}__aggregate as (
    {# This is the most important CTE. Instead of joining all relevant information
    and THEN aggregating, we are instead aggregating from the beginning and then 
    joining downstream for performance. Additionally, we're using a subquery instead 
    of a CTE, which was significantly more performant during our testing. #}
    select
        date_{{grain}},
        {# This is the consistent code you'll find that loops through the list of 
        dimensions. It is used throughout this macro, with slight differences to 
        account for comma syntax around loop last #}
        {% for dim in dimensions %}
            {%- if metrics.is_dim_from_model(metric, dim) -%}
                    {{ dim }},
            {% endif -%}
        {%- endfor %}
        {# This line performs the relevant aggregation by calling the 
        aggregate_primary_metric macro. Take a look at that one if you're curious #}
        {{- metrics.aggregate_primary_metric(metric.type, 'property_to_aggregate') }} as {{ metric.name }},
        {{ dbt_utils.bool_or('metric_date_day is not null') }} as has_data
    from (
        {# This is the "base" CTE which selects the fields we need to correctly 
        calculate the metric.  #}
        select 
            {# This section looks at the sql aspect of the metric and ensures that 
            the value input into the macro is accurate #}
            to_date({{metric.timestamp}}) as metric_date_day, -- timestamp field
            {{calendar_tbl}}.date_{{ grain }} as date_{{grain}},
            -- ALL DIMENSIONS
            {% for dim in dimensions %}
                {%- if metrics.is_dim_from_model(metric, dim) -%}
                    {{ dim }},
                {% endif -%}
            {%- endfor %}
            {%- if metric.sql and metric.sql | replace('*', '') | trim != '' -%}
                {{ metric.sql }} as property_to_aggregate
            {%- elif metric.type == 'count' -%}
                1 as property_to_aggregate /*a specific expression to aggregate wasn't provided, so this effectively creates count(*) */
            {%- else -%}
                {%- do exceptions.raise_compiler_error("Expression to aggregate is required for non-count aggregation in metric `" ~ metric.name ~ "`") -%}  
            {%- endif %}
        from {{ model }}
        left join {{calendar_tbl}} on to_date({{metric.timestamp}}) = date_day
        where 1=1
        
        -- metric start/end dates also applied here to limit incoming data
        {% if start_date or end_date%}
            and (
            {% if start_date and end_date %}
                {{metric.timestamp}} >= cast('{{ start_date }}' as date)
                and {{metric.timestamp}} <= cast('{{ end_date }}' as date)
            {% elif start_date and not end_date %}
                {{metric.timestamp}} >= cast('{{ start_date }}' as date)
            {% elif end_date and not start_date %}
                {{metric.timestamp}} <= cast('{{ end_date }}' as date)
            {% endif %} 
            )      
        {% endif %} 

        -- metric where clauses...
        {% if metric.filters %}
        and (
            {%- for filter in metric.filters %}
                {{ filter.field }} {{ filter.operator }} {{ filter.value }}
                {% if not loop.last %} and {% endif %}
            {%- endfor %}
        )
        {% endif%}


        -- metric where clauses...
        {% if where %}
        and (
            {%- for filter in where %}
                {{ filter }}
                {% if not loop.last %} and {% endif %}
            {%- endfor %}
        )
        {% endif %}
    )
    {#- /* Add 1 twice to account for 1) timeseries dim and 2) to be inclusive of the last dim */ #}
    group by {{ range(1, (dimensions | length) + (relevant_periods | length) + 1 + 1) | join (", ") }}
)
,
{{metric.name}}__dims as (
    select distinct
        {% for dim in dimensions %}
            {%- if metrics.is_dim_from_model(metric, dim) -%}
                {{ dim }}
                {% if not loop.last %},{% endif %}
            {% endif -%}
        {%- endfor %}
    from {{metric.name}}__aggregate
)
,
{{metric.name}}__spine_time as (

    select
        date_{{grain}},

        {# I don't believe the following section is needed because we don't need other
        time periods #}
        {# {% for period in relevant_periods %}
            date_{{ period }},
        {% endfor %} #}

        {% for dim in dimensions %}
            {%- if metrics.is_dim_from_model(metric, dim) -%}
                {{ dim }}
                {% if not loop.last %},{% endif %}
            {% endif -%}
        {%- endfor %}
    from calendar
    cross join {{metric.name}}__dims
)
,
{{metric.name}}__joined as (
    select
        date_{{grain}},
        {% for dim in dimensions %}
            {%- if metrics.is_dim_from_model(metric, dim) -%}
                {{ dim }},
            {% endif -%}
        {%- endfor %}
        ifnull({{metric.name}}, 0) as {{metric.name}}
        
    from {{metric.name}}__spine_time
    left outer join {{metric.name}}__aggregate
        using(date_{{grain}}, 
                {% for dim in dimensions %}
                    {%- if metrics.is_dim_from_model(metric, dim) -%}
                        {{ dim }}
                        {% if not loop.last %},{% endif %}
                    {% endif -%}
                {%- endfor %})

)

select * from {{metric.name}}__joined

{% endmacro %}