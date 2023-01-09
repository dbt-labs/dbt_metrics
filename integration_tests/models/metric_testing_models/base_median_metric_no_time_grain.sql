select *
from 
{{ metrics.calculate(metric('base_median_metric')) 
}}