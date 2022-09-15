select *
from 
{{ metrics.calculate(
    metric('derived_metric'), 
    grain='day', 
    dimensions=['had_discount','order_country','is_weekend'],
    start_date = '2022-01-01',
    end_date = '2022-01-10'
    ) 
}}