select *
from 
{{ metrics.calculate(
    [metric('expression_metric'), metric('metric_on_expression_metric')], 
    grain='day', 
    dimensions=['had_discount']) 
}}