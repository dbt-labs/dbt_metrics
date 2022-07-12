select *
from 
{{ metrics.metric(
    metric('base_count_metric'), 
    grain='month', 
    dimensions=['has_messaged','is_active_past_quarter'], 
    start_date = '2021-02-01'
    )
}}