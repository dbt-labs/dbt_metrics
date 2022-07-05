select *
from 
{{ metrics.metric(metric('base_sum_metric'), 
    grain='day', 
    dimensions=['had_discount']) 
}}