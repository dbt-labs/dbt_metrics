select *
from 
{{ metrics.calculate(metric('base_count_distinct_metric'), 
    grain='month'
    ) 
}}