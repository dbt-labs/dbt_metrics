select *
from 
{{ metrics.metric(metric('revenue'), 
    grain='day', 
    dimensions=['had_discount']) 
}}