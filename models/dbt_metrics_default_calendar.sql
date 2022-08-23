{{ config(materialized='table') }}

with days as (
    {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('1990-01-01' as date)",
    end_date="cast('2030-01-01' as date)"
   )
    }}
),

final as (
    select 
        cast(date_day as date) as date_day,
        cast({{ dbt_utils.date_trunc('week', 'date_day') }} as date) as date_week,
        cast({{ dbt_utils.date_trunc('month', 'date_day') }} as date) as date_month,
        cast({{ dbt_utils.date_trunc('quarter', 'date_day') }} as date) as date_quarter,
        cast({{ dbt_utils.date_trunc('year', 'date_day') }} as date) as date_year
    from days
)

select * from final
