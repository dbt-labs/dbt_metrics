select 
    *
    ,1 as discount_total
from {{ref('fact_orders_source')}}