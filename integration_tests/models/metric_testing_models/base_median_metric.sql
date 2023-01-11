select *
from 
{{ metrics.calculate(metric('base_median_metric'), 
    grain='month', 
    dimensions=['had_discount'],
    date_alias='date') 
}}