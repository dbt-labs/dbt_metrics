select *
from 
{{ metrics.calculate(metric('base_count_metric'), 
    grain='week'
    )
}}