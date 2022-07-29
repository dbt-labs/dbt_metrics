select *
from 
{{ metrics.calculate(
    metric('base_count_metric'), 
    grain='month', 
    start_date = '2021-02-01'
    )
}}