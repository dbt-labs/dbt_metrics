select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric'),metric('base_test_metric')], 
    grain='all_time', 
    dimensions=['had_discount','is_weekend']) 
}}