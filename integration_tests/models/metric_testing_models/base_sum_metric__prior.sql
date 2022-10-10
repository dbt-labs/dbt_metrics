select *
from 
{{ metrics.calculate(
    metric('base_sum_metric'), 
    grain='week', 
    dimensions=['had_discount'],
    secondary_calculations=[metrics.prior(interval=3)] 
    ) 
}}