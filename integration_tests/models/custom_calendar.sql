with days as (
    

with rawdata as (


    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
     + 
    
    p12.generated_number * power(2, 12)
     + 
    
    p13.generated_number * power(2, 13)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
     cross join 
    
    p as p12
     cross join 
    
    p as p13
    
    )

    select *
    from unioned
    where generated_number <= 14610
    order by generated_number

),

all_periods as (

    select (
        

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        cast('1990-01-01' as date)
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= cast('2030-01-01' as date)

)

select * from filtered


),

final as (
    select 
        cast(date_day as date) as date_day,
        {% if target.type == 'bigquery' %}
            --BQ starts its weeks on Sunday. I don't actually care which day it runs on for auto testing purposes, just want it to be consistent with the other seeds
            cast({{ date_trunc('week(MONDAY)', 'date_day') }} as date) as date_week,
        {% else %}
            cast({{ date_trunc('week', 'date_day') }} as date) as date_week,
        {% endif %}
        cast({{ date_trunc('month', 'date_day') }} as date) as date_month,
        cast({{ date_trunc('quarter', 'date_day') }} as date) as date_quarter,
        '2022-01-01' as date_test,
        cast({{ date_trunc('year', 'date_day') }} as date) as date_year,
        true as is_weekend
    from days
)

select * from final
