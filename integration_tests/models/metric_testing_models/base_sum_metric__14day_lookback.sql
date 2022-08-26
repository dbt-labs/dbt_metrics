select *
from 
{{ metrics.calculate(
    metric('base_sum_metric__14day_lookback'), 
    grain='day', 
    dimensions=['had_discount','is_weekend']) 
}}