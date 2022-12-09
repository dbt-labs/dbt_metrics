select *
from 
{{ metrics.calculate(metric('ratio_metric')
    )
}}