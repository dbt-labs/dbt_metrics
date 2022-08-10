select *
from 
{{ metrics.metric(
    metric_name='base_average_metric', 
    grain='month', 
    dimensions=['had_discount']) 
}}