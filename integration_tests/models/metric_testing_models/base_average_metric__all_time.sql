select *
from 
{{ metrics.calculate(metric('base_average_metric'), 
    dimensions=['had_discount']) 
}}