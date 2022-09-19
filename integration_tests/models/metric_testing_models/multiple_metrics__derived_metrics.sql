select *
from 
{{ metrics.calculate(
    [metric('derived_metric'), metric('metric_on_derived_metric')], 
    grain='day', 
    dimensions=['had_discount']) 
}}