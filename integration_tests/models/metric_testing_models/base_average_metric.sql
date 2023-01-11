select *
from 
{{ metrics.calculate(metric('base_average_metric'), 
    grain='test', 
    dimensions=['had_discount']) 
}}