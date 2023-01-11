select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric__no_timestamp')]
    ,dimensions=['had_discount']
    ) 
}}