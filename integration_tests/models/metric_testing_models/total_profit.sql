select *
from 
{{ metrics.metric(metric('total_profit'), 
    grain='day', 
    dimensions=['had_discount','order_country','is_weekend'],
    start_date = '2022-01-01',
    end_date = '2022-01-10') 
}}