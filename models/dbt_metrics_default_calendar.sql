{{ config(materialized='table') }}

--TODO: Don't want to depend on utils long term.
with days as (
    {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2010-01-01' as date)",
    end_date="cast('2030-01-01' as date)"
   )
    }}
),

final as (
    select 
        date_day, 
        {{ dbt_utils.date_trunc('week', 'date_day') }} as date_week,
        {{ dbt_utils.date_trunc('month', 'date_day') }} as date_month,
        {{ dbt_utils.date_trunc('quarter', 'date_day') }} as date_quarter,
        {{ dbt_utils.date_trunc('year', 'date_day') }} as date_year
    from days
)

select * from final