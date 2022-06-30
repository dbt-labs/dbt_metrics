select *
from 
{{ metrics.metric(
    metric('slack_joiners'), 
    grain='month', 
    dimensions=['has_messaged','is_active_past_quarter'], 
    start_date = '2021-02-01'
    )
}}