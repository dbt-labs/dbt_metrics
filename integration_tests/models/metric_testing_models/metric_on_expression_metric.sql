select *
from 
{{ dbt_metrics.calculate(
    metric('metric_on_expression_metric'), 
    grain='day', 
    dimensions=['had_discount','order_country','is_weekend'],
    start_date = '2022-01-01',
    end_date = '2022-01-05'
    
    ) 
}}