select *
from 
{{ dbt_metrics.calculate(metric('base_count_metric'), 
    grain='week'
    )
}}