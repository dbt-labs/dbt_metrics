select *
from 
{{ metrics.calculate(metric('base_average_metric'), 
    grain='all_time',
    dimensions=['had_discount']) 
}}