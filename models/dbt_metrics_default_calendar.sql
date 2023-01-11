{{ config(materialized='table') }}

with days as (
    {{ metrics.metric_date_spine(
    datepart="day",
    start_date="cast('1990-01-01' as date)",
    end_date="cast('2030-01-01' as date)"
   )
    }}
),

final as (
    select 
        cast(date_day as date) as date_day,{% if target.type is 'bigquery' %}
        cast({{ date_trunc('isoweek', 'date_day') }} as date) as date_week,{% else %}
        cast({{ date_trunc('week', 'date_day') }} as date) as date_week,{% endif %}
        cast({{ date_trunc('month', 'date_day') }} as date) as date_month,
        cast({{ date_trunc('quarter', 'date_day') }} as date) as date_quarter,
        cast({{ date_trunc('year', 'date_day') }} as date) as date_year
    from days
)

select * from final
