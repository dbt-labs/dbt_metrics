select *
from 
{{ metrics.calculate(
    [metric('derived_metric'),metric('base_count_distinct_metric')], 
    grain='day',
    dimensions=['had_discount','order_country']
    ) 
}}