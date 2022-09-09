select *
from 
{{ dbt_metrics.calculate(metric('ratio_metric'), 
    grain='all_time'
    )
}}