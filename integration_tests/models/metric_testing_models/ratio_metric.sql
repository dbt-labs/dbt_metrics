select *
from 
{{ metrics.calculate(metric('ratio_metric'), 
    grain='all_time'
    )
}}