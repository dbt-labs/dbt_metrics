select *
from 
{{ metrics.calculate(
    [metric('base_median_metric'),metric('base_average_metric')], 
    grain='month', 
    dimensions=['had_discount'],
    date_alias='dat') 
}}