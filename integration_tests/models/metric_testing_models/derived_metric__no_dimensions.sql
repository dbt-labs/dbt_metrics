select *
from 
{{ metrics.calculate(
    metric('derived_metric'), 
    grain='day', 
    start_date = '2022-01-01',
    end_date = '2022-01-10') 
}}