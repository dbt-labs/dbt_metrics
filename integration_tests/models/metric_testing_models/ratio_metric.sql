select *
from 
{{ dbt_metrics.calculate(metric('ratio_metric'), 
    grain='month'
    )
}}