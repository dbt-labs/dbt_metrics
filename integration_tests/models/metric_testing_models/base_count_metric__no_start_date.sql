select *
from 
{{ metrics.calculate(
    metric('base_count_metric'), 
    grain='month', 
    end_date = '2021-03-01'
    )
}}