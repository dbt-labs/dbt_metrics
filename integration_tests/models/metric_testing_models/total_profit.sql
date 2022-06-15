select *
from 
{{ metrics.metric(metric('total_profit'), 
    grain='day', 
    dimensions=['had_discount','order_country']) 
}}