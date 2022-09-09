select *
from 
{{ dbt_metrics.calculate(metric('base_average_metric'), 
    grain='month', 
    dimensions=['had_discount']) 
}}