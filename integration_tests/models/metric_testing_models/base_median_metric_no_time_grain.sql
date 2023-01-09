select *
from 
{{ metrics.calculate(metric('base_median_metric'), 
    dimensions=['had_discount']) 
}}