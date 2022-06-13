select *
from 
{{ metrics.metric(metric('sum_order_total'), 
    grain='day', 
    dimensions=['had_discount']) 
}}