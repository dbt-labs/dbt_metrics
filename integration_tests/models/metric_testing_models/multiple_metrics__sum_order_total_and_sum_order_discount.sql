select *
from 
{{ metrics.metric(
    [metric('sum_order_total'), metric('sum_discount_total')],
    grain='day', 
    dimensions=['had_discount']) 
}}