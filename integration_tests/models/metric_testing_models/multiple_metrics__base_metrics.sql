select *
from 
{{ metrics.metric(
    [metric('base_sum_metric'), metric('base_average_metric')], 
    grain='day', 
    dimensions=['had_discount']) 
}}